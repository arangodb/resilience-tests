/* global describe, it, before, after */
'use strict';
const InstanceManager = require('../InstanceManager.js');
const expect = require('chai').expect;
const arangojs = require('arangojs');
const rp = require('request-promise');

describe('Move shards', function () {
  let instanceManager = new InstanceManager('shard-move');
  let db;
  let servers = [];
  before(function () {
    return instanceManager.startCluster(1, 2, 3)
    .then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: '_system'
      });
      return db.collection('testcollection').create({shards: 1, replicationFactor: 2});
    })
    .then(() => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_db/_system/_admin/aardvark/cluster/DBServers',
        json: true,
      })
    })
    .then(_servers => {
      servers = _servers;
    });
  });
  after(function () {
    return;// instanceManager.cleanup();
  });

  it('should allow moving shards while writing', function() {
    let stopMoving = false;

    let moveShards = function() {
      if (moveShards.stop) {
        return Promise.resolve();
      }

      let waitServersIs = function(should, num) {
        if (num > 100) {
          return Promise.reject("Shard did not come into sync after " + num + " tries. " + JSON.stringify(should));
        }
        return rp({
          url: instanceManager.getEndpointUrl() + '/_db/_system/_admin/cluster/shardDistribution',
          json: true,
        })
        .then(shardDistribution => {
          let shards = shardDistribution.results.testcollection.Current;
          let shardKey = Object.keys(shards)[0];
          return [shards[shardKey].leader].concat(shards[shardKey].followers);
        })
        .then(is => {
          return is.length == should.length
            && is.every((server, index) => should[index] == server);
        })
        .then(equal => {
          if (!equal) {
            return new Promise((resolve, reject) => {
              setTimeout(resolve, 100);
            })
            .then(() => {
              return waitServersIs(should, num + 1);
            });
          }
        });
      };

      return rp({
        url: instanceManager.getEndpointUrl() + '/_db/_system/_admin/cluster/shardDistribution',
        json: true,
      })
      .then(shardDistribution => {
        let shards = shardDistribution.results.testcollection.Plan;
        let shardKey = Object.keys(shards)[0];
        let is = [shards[shardKey].leader].concat(shards[shardKey].followers);
        let freeServer = servers.filter(server => is.indexOf(server.name) == -1)[0];
        let should = is.slice();
        should[0] = freeServer.name;
        let move = {
          collection: 'testcollection',
          database: '_system',
          shard: shardKey,
          fromServer: servers.filter(server => server.name == shards[shardKey].leader)[0].id,
          toServer: freeServer.id,
        };
        return rp({
          url: instanceManager.getEndpointUrl() + '/_db/_system/_admin/cluster/moveShard',
          json: true,
          body: move,
          method: 'POST',
        })
        .then(() => {
          return should;
        });
      })
      .then(should => {
        return waitServersIs(should);
      })
      .then(() => {
        return moveShards();
      });
    }

    let insertDocuments = function() {
      return [...Array(10000).keys()].reduce((promise, key) => {
        return promise.then(() => {
          return db.collection('testcollection').save({"hallooo": key});
        });
      }, Promise.resolve())
    };

    let movePromise = moveShards();

    return insertDocuments()
    .then(() => {
      moveShards.stop = true;
      return movePromise;
    })
    .then(() => {
      return db.collection('testcollection').count();
    })
    .then(count => {
      expect(count.count).to.equal(10000);
      return db.collection('testcollection').all();
    })
    .then(cursor => {
      return cursor.all();
    })
    .then(all => {
      all = all.map(doc => doc.hallooo);
      all.sort((a, b) => a < b ? -1 : 1);
      expect(all).to.deep.equal([...Array(10000).keys()]);
    });
  });
});
