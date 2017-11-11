/* global describe, it, beforeEach, afterEach */
'use strict';
const join = require('path').join;
const readFileSync = require('fs').readFileSync;
const InstanceManager = require('../../InstanceManager.js');
const arangojs = require('arangojs');
const expect = require('chai').expect;

const noop = () => {};
const service1 = readFileSync(join(__dirname, '..', '..', 'fixtures', 'service1.zip'));
const service2 = readFileSync(join(__dirname, '..', '..', 'fixtures', 'service2.zip'));

describe('Foxx service', function () {
  const im = new InstanceManager();
  const MOUNT = '/resiliencetestservice';

  beforeEach(() => im.startCluster(1, 2, 2));
  afterEach(() => im.cleanup().catch(noop));

  describe('when already installed', function () {
    beforeEach(function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return db.installService(MOUNT, service1)
      .then(() => db.route(MOUNT).get())
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });

    it('should survive primary dbServer being rebooted', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return im.shutdown(dbServer)
        .then(() => im.restart(dbServer));
      })
      .then(() => db.route(MOUNT).get())
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });

    it('should survive primary dbServer being replaced', function () {
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return im.destroy(dbServer)
        .then(() => im.replace(dbServer));
      })
      .then(() => {
        const coord = im.coordinators()[0];
        const db = arangojs(im.getEndpointUrl(coord));
        return db.route(MOUNT).get();
      })
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });

    it('should survive a single dbServer being added', function () {
      return im.startDbServer('dbServer-new')
      .then((instance) => im.waitForInstance(instance))
      .then((instance) => {
        im.instances = [...im.instances, instance];
        const coord = im.coordinators()[0];
        const db = arangojs(im.getEndpointUrl(coord));
        return db.route(MOUNT).get();
      })
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });

    it('should survive all dbServers being rebooted', function () {
      const instances = im.dbServers();
      return Promise.all(instances.map(instance => im.shutdown(instance)))
      .then(() => Promise.all(instances.map(instance => im.restart(instance))))
      .then(() => {
        const coord = im.coordinators()[0];
        const db = arangojs(im.getEndpointUrl(coord));
        return db.route(MOUNT).get();
      })
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });
  });

  describe('while primary dbServer is being rebooted', function () {
    it('can be installed', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return im.shutdown(dbServer)
        .then(() => db.installService(MOUNT, service1))
        .then(() => im.restart(dbServer));
      })
      .then(() => db.route(MOUNT).get())
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });

    it('can be replaced', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return db.installService(MOUNT, service1)
        .then(() => im.shutdown(dbServer))
        .then(() => db.replaceService(MOUNT, service2))
        .then(() => im.restart(dbServer));
      })
      .then(() => db.route(MOUNT).get())
      .then((response) => {
        expect(response).to.have.property('body', 'service2');
      });
    });

    it('can be removed', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return db.installService(MOUNT, service1)
        .then(() => im.shutdown(dbServer))
        .then(() => db.uninstallService(MOUNT))
        .then(() => im.restart(dbServer));
      })
      .then(() => {
        return db.route(MOUNT).get()
        .then(
          () => expect.fail(),
          (error) => expect(error).to.have.property('code', 404)
        );
      });
    });
  });

  describe('while primary dbServer is being replaced', function () {
    it('can be installed', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return im.destroy(dbServer)
        .then(() => db.installService(MOUNT, service1))
        .then(() => im.replace(dbServer));
      })
      .then(() => db.route(MOUNT).get())
      .then((response) => {
        expect(response).to.have.property('body', 'service1');
      });
    });

    it('can be replaced', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return db.installService(MOUNT, service1)
        .then(() => im.destroy(dbServer))
        .then(() => db.replaceService(MOUNT, service2))
        .then(() => im.replace(dbServer));
      })
      .then(() => db.route(MOUNT).get())
      .then((response) => {
        expect(response).to.have.property('body', 'service2');
      });
    });

    it('can be removed', function () {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im.findPrimaryDbServer('_apps')
      .then((dbServer) => {
        return db.installService(MOUNT, service1)
        .then(() => im.destroy(dbServer))
        .then(() => db.uninstallService(MOUNT))
        .then(() => im.replace(dbServer));
      })
      .then(() => {
        return db.route(MOUNT).get()
        .then(
          () => expect.fail(),
          (error) => expect(error).to.have.property('code', 404)
        );
      });
    });
  });
});
