/*global print, arango, describe, it */
'use strict';

const InstanceManager = require('../InstanceManager.js');
const expect = require('chai').expect;
const arangojs = require('arangojs');

describe('ClusterResilience', function() {
  let instanceManager = new InstanceManager('cluster_resilience');;
  let db;
  before(function() {
    return instanceManager.startCluster(3, 2, 2)
    .then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: '_system',
      });
    });
  })

  afterEach(function() {
    return instanceManager.check();
  })

  after(function() {
    return instanceManager.cleanup();
  })

  it('should setup and teardown the cluster properly', function() {
  })

  it('should report the same number of documents after a server restart', function() {
    let count = 7;
    return db.collection('testcollection').create({ shards: 4})
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
      .then(() => {
        return db.collection('testcollection').count();
      })
      .then(realCount => {
        expect(realCount.count).to.equal(count);
      })
      .then(() => {
        let dbServer = instanceManager.dbServers()[0];
        return instanceManager.kill(dbServer)
        .then(() => {
          return dbServer;
        });
      })
      .then(dbServer => {
        // mop: wait a bit to possibly make the cluster go wild!
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve(dbServer);
          }, 1000);
        });
      })
      .then(dbServer => {
        return instanceManager.restart(dbServer);
      })
      .then(() => {
        return db.collection('testcollection').count();
      })
      .then(realCount => {
        expect(realCount.count).to.equal(count);
      })
    })
  })
});
