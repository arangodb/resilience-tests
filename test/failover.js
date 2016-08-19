'use strict';

const InstanceManager = require('../InstanceManager.js');
const expect = require('chai').expect;
const arangojs = require('arangojs');

describe('Failover', function() {
  let instanceManager = new InstanceManager('failover');;
  let db;
  before(function() {
    return instanceManager.startCluster(1, 2, 3)
    .then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: '_system',
      });
      return db.collection('testcollection').create({ shards: 4, replicationFactor: 2})
      .then(() => {
        return Promise.all([
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
        ])
      })
    });
  })
  
  after(function() {
    return instanceManager.cleanup();
  })
  it('should fail over to another replica when a server goes down', function() {
    let dbServer = instanceManager.dbServers()[0];
    return instanceManager.kill(dbServer)
    .then(server => {
      // mop: wait a bit to possibly make the cluster go wild!
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          resolve(server);
        }, 1000);
      });
    })
    .then(() => {
      return db.collection('testcollection').count();
    })
    .then(count => {
      expect(count.count).to.equal(7);
    })
  });
});
