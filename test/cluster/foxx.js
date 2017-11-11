/* global describe, it, beforeEach, afterEach */
'use strict';
const join = require('path').join;
const readFileSync = require('fs').readFileSync;
const InstanceManager = require('../../InstanceManager.js');
const arangojs = require('arangojs');
const expect = require('chai').expect;

const noop = () => {};
const service1 = readFileSync(
  join(__dirname, '..', '..', 'fixtures', 'service1.zip')
);
const service2 = readFileSync(
  join(__dirname, '..', '..', 'fixtures', 'service2.zip')
);

describe('Foxx service', function() {
  const im = new InstanceManager();
  const MOUNT = '/resiliencetestservice';

  beforeEach(() => im.startCluster(1, 2, 2));
  afterEach(() => im.cleanup().catch(noop));

  describe('when already installed', function() {
    beforeEach(function() {
      const coord = im.coordinators()[1];
      const db = arangojs(im.getEndpointUrl(coord));
      return db
        .installService(MOUNT, service1)
        .then(() => db.route(MOUNT).get())
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('should survive a single coordinator being rebooted', function() {
      const coord = im.coordinators()[0];
      const db = arangojs(im.getEndpointUrl(coord));
      return im
        .shutdown(coord)
        .then(() => im.restart(coord))
        .then(() => db.route(MOUNT).get())
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('should survive a single coordinator being replaced', function() {
      const coord1 = im.coordinators()[0];
      return im
        .destroy(coord1)
        .then(() => im.replace(coord1))
        .then(coord2 => {
          const db = arangojs(im.getEndpointUrl(coord2));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('should survive a single coordinator being added', function() {
      return im
        .startCoordinator('coordinator-new')
        .then(instance => im.waitForInstance(instance))
        .then(instance => {
          im.instances = [...im.instances, instance];
          const db = arangojs(im.getEndpointUrl(instance));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('should survive all coordinators being replaced', function() {
      const instances = im.coordinators();
      return Promise.all(instances.map(instance => im.destroy(instance)))
        .then(() =>
          Promise.all(instances.map(instance => im.replace(instance)))
        )
        .then(instances => {
          const coord = instances[0];
          const db = arangojs(im.getEndpointUrl(coord));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('should survive all coordinators being rebooted', function() {
      const instances = im.coordinators();
      return Promise.all(instances.map(instance => im.shutdown(instance)))
        .then(() =>
          Promise.all(instances.map(instance => im.restart(instance)))
        )
        .then(() => {
          const coord = im.coordinators()[0];
          const db = arangojs(im.getEndpointUrl(coord));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });
  });

  describe('while a single coordinator is being rebooted', function() {
    it('can be installed', function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      return im
        .shutdown(coord2)
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord1));
          return db.installService(MOUNT, service1);
        })
        .then(() => im.restart(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord2));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('can be replaced', function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      const db = arangojs(im.getEndpointUrl(coord1));
      return db
        .installService(MOUNT, service1)
        .then(() => im.shutdown(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord1));
          return db.replaceService(MOUNT, service2);
        })
        .then(() => im.restart(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord2));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service2');
        });
    });

    it('can be removed', function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      const db = arangojs(im.getEndpointUrl(coord1));
      return db
        .installService(MOUNT, service1)
        .then(() => im.shutdown(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord1));
          return db.uninstallService(MOUNT);
        })
        .then(() => im.restart(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord2));
          return db
            .route(MOUNT)
            .get()
            .then(
              () => expect.fail(),
              error => expect(error).to.have.property('code', 404)
            );
        });
    });
  });

  describe('while a single coordinator is being replaced', function() {
    it('can be installed', function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      return im
        .destroy(coord2)
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord1));
          return db.installService(MOUNT, service1);
        })
        .then(() => im.replace(coord2))
        .then(coord3 => {
          const db = arangojs(im.getEndpointUrl(coord3));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service1');
        });
    });

    it('can be replaced', function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      const db = arangojs(im.getEndpointUrl(coord1));
      return db
        .installService(MOUNT, service1)
        .then(() => im.destroy(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord1));
          return db.replaceService(MOUNT, service2);
        })
        .then(() => im.replace(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord2));
          return db.route(MOUNT).get();
        })
        .then(response => {
          expect(response).to.have.property('body', 'service2');
        });
    });

    it('can be removed', function() {
      const coord1 = im.coordinators()[0];
      const coord2 = im.coordinators()[1];
      const db = arangojs(im.getEndpointUrl(coord1));
      return db
        .installService(MOUNT, service1)
        .then(() => im.destroy(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord1));
          return db.uninstallService(MOUNT);
        })
        .then(() => im.replace(coord2))
        .then(() => {
          const db = arangojs(im.getEndpointUrl(coord2));
          return db
            .route(MOUNT)
            .get()
            .then(
              () => expect.fail(),
              error => expect(error).to.have.property('code', 404)
            );
        });
    });
  });
});
