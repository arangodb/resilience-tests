/* global describe, it, afterEach */
'use strict';
const InstanceManager = require('../InstanceManager.js');
const expect = require('chai').expect;
const arangojs = require('arangojs');

describe('Setup', function () {
  let instanceManager = new InstanceManager('setup');
  it('should be possible to stop and restart a cluster', function () {
    let db;
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: '_system'
      });
      return db.collection('testcollection').create({numberOfShards: 4});
    })
    .then(() => {
      return instanceManager.shutdownCluster();
    })
    .then(() => {
      return Promise.all(instanceManager.instances.map(instance => {
        return instanceManager.restart(instance);
      }));
    })
    .then(() => {
      return db.collection('testcollection').count();
    })
    .then(result => {
      expect(result.count).to.equal(0);
    });
  });

  afterEach(function () {
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup();
  });
});
