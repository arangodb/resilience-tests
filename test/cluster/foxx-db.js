/* global describe, it, beforeEach, afterEach */
"use strict";
const join = require("path").join;
const readFileSync = require("fs").readFileSync;
const InstanceManager = require("../../InstanceManager.js");
const arangojs = require("arangojs");
const expect = require("chai").expect;
const FailoverError = InstanceManager.FailoverError;
// Wait 100s this is rather long and should retain on slow machines also
const MAX_FAILOVER_TIMEOUT_MS = 1000000;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const noop = () => {};
const service1 = readFileSync(
  join(__dirname, "..", "..", "fixtures", "service1.zip")
);
const service2 = readFileSync(
  join(__dirname, "..", "..", "fixtures", "service2.zip")
);
const retryIntervalMS = 10000;

const debugLog = (...args) => {
  if (process.env.LOG_IMMEDIATE === "1") {
    console.log(new Date().toISOString(), ' ', ...args);
  }
};

describe("Foxx service (dbserver)", function() {
  const im = InstanceManager.create();
  const MOUNT = "/resiliencetestservice";

  const waitForLeaderFailover = async function(col, lastLeader) {
    let count = 0;
    while (count * retryIntervalMS < MAX_FAILOVER_TIMEOUT_MS) {
      try {
        let newLeader = await im.findPrimaryDbServer(col);
        if (newLeader !== lastLeader) {
          // we got a new leader yay \o/
          return;
        }
      } catch (e) {
        if (e instanceof FailoverError) {
          // This is expected! just continue
          debugLog(`waitForLeaderFailover: caught expected FailoverError ${e}`);
        } else if (e instanceof Error && e.message.startsWith('Unknown endpoint ')) {
          // This is expected! just continue
          debugLog(`waitForLeaderFailover: caught expected Error ${e}`);
        } else {
          // unexpected error throw it
          throw e;
        }
      }
      ++count;
      await sleep(retryIntervalMS);
    }
    console.error("Failover did not happen. Now dumping the Agency State");
    await im.dumpAgency();
    throw new Error(
      `Failover did not succueed in ${MAX_FAILOVER_TIMEOUT_MS / 1000}s`
    );
  };

  const waitForLazyCreatedCollections = async function() {
    let count = 0;
    while (count * retryIntervalMS < MAX_FAILOVER_TIMEOUT_MS) {
      try {
        await im.findPrimaryDbServer("_statistics");
        await im.findPrimaryDbServer("_statistics15");
        await im.findPrimaryDbServer("_statisticsRaw");
        // If we get here we found all three collections
        // Everything we wanted
        break;
      } catch (e) {
        ++count;
        await sleep(retryIntervalMS);
        // Expected, collections may not be created yet. We wait
      }
    }
    if (count * retryIntervalMS >= MAX_FAILOVER_TIMEOUT_MS) {
      expect(true).to.equal(
        false,
        "Did not create statistics collections in a timely manner"
      );
    }
    await im.waitForSyncReplication();
  };

  beforeEach(async () => {
    await im.startCluster(1, 2, 2);
    await im.waitForSyncReplication();
  });

  afterEach(async () => {
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    const retainDir = currentTest.state === "failed";
    try {
      await im.cleanup(retainDir);
    } catch (e) {
      console.error(`Error during cleanup: ${e}`);
    }
  });

  describe("when already installed", function() {
    beforeEach(async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      await db.installService(MOUNT, service1);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive primary dbServer being rebooted", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer("_apps");
      await im.shutdown(primary);
      await im.restart(primary);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive primary dbServer being replaced", async function() {
      const primary = await im.findPrimaryDbServer("_apps");
      await im.destroy(primary);
      // replace with new endpoint
      await im.replace(primary, true);
      await waitForLeaderFailover("_apps", primary);
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive a single dbServer being added", async function() {
      const instance = await im.startDbServer("dbServer-new");
      await InstanceManager.waitForInstance(instance);
      im.instances = [...im.instances, instance];
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive all dbServers being rebooted", async function() {
      const instances = im.dbServers();
      await Promise.all(instances.map(instance => im.shutdown(instance)));
      await Promise.all(instances.map(instance => im.restart(instance)));
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });
  });

  describe("while primary dbServer is being rebooted", function() {
    beforeEach(async () => {
      await waitForLazyCreatedCollections();
    });

    it("can be installed", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer("_apps");
      await im.shutdown(primary);
      await waitForLeaderFailover("_apps", primary);
      await db.installService(MOUNT, service1);
      await im.restart(primary);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("can be replaced", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer("_apps");
      await db.installService(MOUNT, service1);

      await im.shutdown(primary);
      await waitForLeaderFailover("_apps", primary);
      await db.replaceService(MOUNT, service2);
      await im.restart(primary);

      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service2");
    });

    it("can be removed", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer("_apps");
      await db.installService(MOUNT, service1);

      await im.shutdown(primary);
      await waitForLeaderFailover("_apps", primary);
      await db.uninstallService(MOUNT);
      await im.restart(primary);

      try {
        await db.route(MOUNT).get();
        expect.fail();
      } catch (error) {
        expect(error).to.have.property("code", 404);
      }
    });
  });

  describe("while primary dbServer is being replaced", function() {
    it("can be installed", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const dbServer = await im.findPrimaryDbServer("_apps");
      await im.destroy(dbServer);
      await waitForLeaderFailover("_apps", dbServer);
      await db.installService(MOUNT, service1);
      await im.replace(dbServer);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("can be replaced", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const dbServer = await im.findPrimaryDbServer("_apps");
      await db.installService(MOUNT, service1);
      await im.destroy(dbServer);
      await waitForLeaderFailover("_apps", dbServer);
      await db.replaceService(MOUNT, service2);
      await im.replace(dbServer);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service2");
    });

    it("can be removed", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const dbServer = await im.findPrimaryDbServer("_apps");
      await db.installService(MOUNT, service1);
      await im.destroy(dbServer);
      await waitForLeaderFailover("_apps", dbServer);
      await db.uninstallService(MOUNT);
      await im.replace(dbServer);
      try {
        const response = await db.route(MOUNT).get();
        expect.fail();
      } catch (error) {
        expect(error).to.have.property("code", 404);
      }
    });
  });
});
