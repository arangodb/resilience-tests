/* global describe, it, before, after */
'use strict';
const InstanceManager = require('../InstanceManager.js');
const expect = require('chai').expect;
const arangojs = require('arangojs');
const rp = require('request-promise');
const fs = require('fs');

describe('Failover', function () {
  let instanceManager = new InstanceManager('failover');
  let db;
  before(function () {
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: '_system'
      });
      return db.collection('testcollection').create({shards: 4, replicationFactor: 2})
      .then(() => {
        return Promise.all([
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()}),
          db.collection('testcollection').save({'testung': Date.now()})
        ]);
      })
      .then(() => {
        let maxTime = Date.now() + 30000;
        let waitForSyncRepl = function() {
          if (Date.now() >= maxTime) {
            return Promise.reject(new Error('Lousy replication didn\'t come into sync after 30s for 7 documents. That is lol'));
          }
          return rp({
            method: 'POST',
            url: instanceManager.getEndpointUrl(instanceManager.agents()[0]) + '/_api/agency/read',
            json: true,
            body: [['/']],
          })
          .then(data => {
            let plan    = data[0].arango.Plan;
            let current = data[0].arango.Current;
            let plannedCollection = Object.keys(plan.Collections['_system']).reduce((result, cid) => {
              if (result) {
                return result;
              }

              if (plan.Collections['_system'][cid].name == 'testcollection') {
                return plan.Collections['_system'][cid];
              }
              return undefined;
            }, undefined);

            let done = Object.keys(plannedCollection.shards).every(shardName => {
              return current.Collections['_system'][plannedCollection.id][shardName].servers.length == 2;
            });
            if (!done) {
              return new Promise((resolve, reject) => {
                setTimeout(resolve, 100);
              })
              .then(() => {
                return waitForSyncRepl();
              });
            }
          });
        };

        return waitForSyncRepl();
      })
    })
  });

  after(function () {
    return instanceManager.cleanup();
  });
  it('should fail over to another replica when a server goes down', function () {
    return rp({
      method: 'POST',
      url: instanceManager.getEndpointUrl(instanceManager.agents()[0]) + '/_api/agency/read',
      json: true,
      body: [['/']],
    })
    .then(data => {
      let plan    = data[0].arango.Plan;

      let plannedCollection = Object.keys(plan.Collections['_system']).reduce((result, cid) => {
        if (result) {
          return result;
        }

        if (plan.Collections['_system'][cid].name == 'testcollection') {
          return plan.Collections['_system'][cid];
        }
        return undefined;
      }, undefined);
      let shardName = Object.keys(plannedCollection.shards)[0];
      let leaderId = plannedCollection.shards[shardName][0];
      let leaderEndpoint = data[0].arango.Current.ServersRegistered[leaderId].endpoint;
      let dbServer = instanceManager.dbServers().reduce((found, server) => {
        if (found) {
          return found;
        }

        if (server.endpoint == leaderEndpoint) {
          return server;
        }

        return undefined;
      }, undefined);
      return instanceManager.kill(dbServer)
    })
    .then(() => {
      return db.collection('testcollection').save({'testung': Date.now()});
    })
    .then(() => {
      return db.collection('testcollection').count();
    })
    .then(count => {
      expect(count.count).to.equal(8);
    })
  });
});
