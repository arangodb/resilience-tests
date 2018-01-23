/* global describe, it, beforeEach, afterEach */
'use strict';
const join = require('path').join;
const readFileSync = require('fs').readFileSync;
const InstanceManager = require('../../InstanceManager.js');
const arangojs = require('arangojs');
const expect = require('chai').expect;
const FailoverError = require('../../Errors.js').FailoverError;
// Wait 100s this is rather long and should retain on slow machines also
// const MAX_FAILOVER_TIMEOUT_MS = 1000000;
const MAX_FAILOVER_TIMEOUT_MS = 10000;
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const noop = () => {};
const service1 = readFileSync(join(__dirname, '..', '..', 'fixtures', 'service1.zip'));
const service2 = readFileSync(join(__dirname, '..', '..', 'fixtures', 'service2.zip'));


describe('Foxx service', function () {
  const im = new InstanceManager();
  const MOUNT = '/resiliencetestservice';

  const waitForLeaderFailover = async function (col, lastLeader) {
    // const retryIntervalMS = 10000;
    const retryIntervalMS = 1000;
    let count = 0;
    while (count * retryIntervalMS < MAX_FAILOVER_TIMEOUT_MS) {
      try {
        let newLeader = await im.findPrimaryDbServer(col);
        if (newLeader != lastLeader) {
          // we got a new leader yay \o/
          return;
        }
        console.error(newLeader,"vs",lastLeader);
      } catch (e) {
        if (e instanceof FailoverError) {
          console.error("Failover Error!");
          // This is expected! just continue
        } else {
          // unexpected error throw it
          throw e;
        }
      }
      ++count;
      await sleep(retryIntervalMS);
    }
    await im.dumpAgency();
    throw new Error(`Failover did not succueed in ${MAX_FAILOVER_TIMEOUT_MS/1000}s`);
  };

  beforeEach(() => im.startCluster(1, 2, 2));
  afterEach(async () => {
    try {
      await im.cleanup();
    } catch (_) {}
  });

  describe('when already installed', function () {
    beforeEach(async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      await db.installService(MOUNT, service1);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

    it('should survive primary dbServer being rebooted', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer('_apps');
      await im.shutdown(primary);
      await im.restart(primary);
      let response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

    it('should survive primary dbServer being replaced', async function () {
      const primary = await im.findPrimaryDbServer('_apps');
      await im.destroy(primary);
      await im.replace(primary);
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

    it('should survive a single dbServer being added', async function () {
      const instance = await im.startDbServer('dbServer-new');
      await im.waitForInstance(instance);
      im.instances = [...im.instances, instance];
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

    it('should survive all dbServers being rebooted', async function () {
      const instances = im.dbServers();
      await Promise.all(instances.map(instance => im.shutdown(instance)));
      await Promise.all(instances.map(instance => im.restart(instance)));
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

  });

  describe('while primary dbServer is being rebooted', function () {

    it.only('can be installed', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer('_apps');
      console.error("Killing: ", primary);
      await im.shutdown(primary);
      console.error("This run worked");
      await waitForLeaderFailover('_apps', primary);
      await db.installService(MOUNT, service1);
      await im.restart(primary);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

    it('can be replaced', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer('_apps');
      await db.installService(MOUNT, service1);

      await im.shutdown(primary);
      await waitForLeaderFailover('_apps', primary);
      await db.replaceService(MOUNT, service2);
      await im.restart(primary);

      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service2');
    });

    it('can be removed', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const primary = await im.findPrimaryDbServer('_apps');
      await db.installService(MOUNT, service1)

      await im.shutdown(primary);
      await waitForLeaderFailover('_apps', primary);
      await db.uninstallService(MOUNT);
      await im.restart(primary);

      try {
        await db.route(MOUNT).get()
        expect.fail();
      } catch (error) {
        expect(error).to.have.property('code', 404)
      }
    });
  });

  describe('while primary dbServer is being replaced', function () {

    it('can be installed', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const dbServer = await im.findPrimaryDbServer('_apps');
      await im.destroy(dbServer);
      await waitForLeaderFailover('_apps', dbServer);
      await db.installService(MOUNT, service1);
      await im.replace(dbServer);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service1');
    });

    it('can be replaced', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const dbServer = await im.findPrimaryDbServer('_apps');
      await db.installService(MOUNT, service1);
      await im.destroy(dbServer);
      await waitForLeaderFailover('_apps', dbServer);
      await db.replaceService(MOUNT, service2);
      await im.replace(dbServer);
      const response = await db.route(MOUNT).get();
      expect(response).to.have.property('body', 'service2');
    });

    it('can be removed', async function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      const dbServer = await im.findPrimaryDbServer('_apps');
      await db.installService(MOUNT, service1);
      await im.destroy(dbServer);
      await waitForLeaderFailover('_apps', dbServer);
      await db.uninstallService(MOUNT);
      await im.replace(dbServer);
      try {
        const response = await db.route(MOUNT).get();
        expect.fail();
      } catch (error) {
        expect(error).to.have.property('code', 404);
      }
    });
  });
});
