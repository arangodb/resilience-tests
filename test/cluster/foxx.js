/* global describe, it, beforeEach, afterEach */
"use strict";
const join = require("path").join;
const readFileSync = require("fs").readFileSync;
const InstanceManager = require("../../InstanceManager.js");
const arangojs = require("arangojs");
const expect = require("chai").expect;
const {sleep, debugLog} = require('../../utils');

const noop = () => {};
const service1 = readFileSync(
  join(__dirname, "..", "..", "fixtures", "service1.zip")
);
const service2 = readFileSync(
  join(__dirname, "..", "..", "fixtures", "service2.zip")
);

describe("Foxx service (coordinator)", function() {
  const im = InstanceManager.create();
  const MOUNT = "/resiliencetestservice";

  beforeEach(async () => {
    await im.startCluster(1, 2, 2);
  });
  afterEach(async () => {
    try {
      const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
      const retainDir = currentTest.state === "failed";
      await im.cleanup(retainDir);
    } catch (err) {
      debugLog(`cleanup failed: ${err}`, err);
    }
  });

  describe("when already installed", function() {
    beforeEach(async function() {
      const coord = im.coordinators()[1];
      const db = arangojs(im.getEndpointUrl(coord));
      await db.installService(MOUNT, service1);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive a single coordinator being rebooted", async function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      await im.shutdown(coord);
      await im.restart(coord);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive a single coordinator being replaced", async function() {
      const coord1 = im.coordinators()[0];
      await im.destroy(coord1);
      const coord2 = await im.replace(coord1);
      const db = arangojs(im.getEndpointUrl(coord2));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive a single coordinator being added", async function() {
      const instance = await im.startCoordinator("coordinator-new");
      await InstanceManager.waitForInstance(instance);
      im.instances = [...im.instances, instance];
      const db = arangojs(im.getEndpointUrl(instance));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive all coordinators being replaced", async function() {
      const instances = im.coordinators();
      try {
        await Promise.all(instances.map(instance => im.destroy(instance)));
        await Promise.all(instances.map(instance => im.replace(instance)));
      } catch (e) {
        expect.fail(null, null, "Failed to replace all coordinators: " + e);
      }
      const coord = instances[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("should survive all coordinators being rebooted", async function() {
      const instances = im.coordinators();
      try {
        await Promise.all(instances.map(instance => im.shutdown(instance)));
        await Promise.all(instances.map(instance => im.restart(instance)));
      } catch (e) {
        expect.fail(null, null, "Failed to restart all coordinators: " + e);
      }
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });
  });

  describe("while a single coordinator is being rebooted", function() {
    it("can be installed", async function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      await im.shutdown(coord2);
      let db = arangojs(im.getEndpointUrl(coord1));
      await db.installService(MOUNT, service1);
      await im.restart(coord2);
      db = arangojs(im.getEndpointUrl(coord2));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("can be replaced", async function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      let db = arangojs(im.getEndpointUrl(coord1));
      await db.installService(MOUNT, service1);
      await im.shutdown(coord2);
      db = arangojs(im.getEndpointUrl(coord1));
      await db.replaceService(MOUNT, service2);
      await im.restart(coord2);
      db = arangojs(im.getEndpointUrl(coord2));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service2");
    });

    it("can be removed", async function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      let db = arangojs(im.getEndpointUrl(coord1));
      await db.installService(MOUNT, service1);
      await im.shutdown(coord2);
      db = arangojs(im.getEndpointUrl(coord1));
      await db.uninstallService(MOUNT);
      await im.restart(coord2);
      db = arangojs(im.getEndpointUrl(coord2));
      let success = true;
      try {
        const response = await db.route(MOUNT).get();
        console.log(response);
      } catch (error) {
        success = false;
        expect(error).to.have.property("code", 404);
      }
      expect(success).to.equal(
        false,
        "The uninstalled service is still reachable!"
      );
    });
  });

  describe("while a single coordinator is being replaced", function() {
    it("can be installed", async function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      await im.destroy(coord2);
      let db = arangojs(im.getEndpointUrl(coord1));
      await db.installService(MOUNT, service1);
      const coord3 = await im.replace(coord2);
      db = arangojs(im.getEndpointUrl(coord3));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service1");
    });

    it("can be replaced", async function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      let db = arangojs(im.getEndpointUrl(coord1));
      await db.installService(MOUNT, service1);
      await im.destroy(coord2);
      db = arangojs(im.getEndpointUrl(coord1));
      await db.replaceService(MOUNT, service2);
      await im.replace(coord2);
      db = arangojs(im.getEndpointUrl(coord2));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property("body", "service2");
    });

    it("can be removed", async function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      let db = arangojs(im.getEndpointUrl(coord1));
      await db.installService(MOUNT, service1);
      await im.destroy(coord2);
      db = arangojs(im.getEndpointUrl(coord1));
      await db.uninstallService(MOUNT);
      await im.replace(coord2);
      db = arangojs(im.getEndpointUrl(coord2));
      try {
        await db.route(MOUNT).get();
        expect.fail();
      } catch (error) {
        expect(error).to.have.property("code", 404);
      }
    });
  });
});
