/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");
const rp = require("request-promise");
const _ = require("lodash");

describe("Move shards", function() {
  let instanceManager = InstanceManager.create();
  let db;
  let servers = [];
  let aTestFailed;
  before(function() {
    aTestFailed = false;
    return instanceManager
      .startCluster(1, 2, 3)
      .then(() => {
        db = arangojs({
          url: instanceManager.getEndpointUrl(),
          databaseName: "_system"
        });
        return db
          .collection("testcollection")
          .create({ shards: 1, replicationFactor: 2 });
      })
      .then(() => {
        return rp({
          url:
            instanceManager.getEndpointUrl() +
            "/_db/_system/_admin/aardvark/cluster/DBServers",
          json: true
        });
      })
      .then(_servers => {
        servers = _servers;
      });
  });
  afterEach(function() {
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    if (currentTest.state === "failed") {
      aTestFailed = true;
    }
  });
  after(function() {
    const retainDir = aTestFailed;
    return instanceManager.cleanup(retainDir);
  });

  it("should allow moving shards while writing", function() {
    let stopMoving = false;

    let moveShards = function() {
      if (moveShards.stop) {
        return Promise.resolve();
      }

      let waitShardMoved = function(newLeader, numServers, num) {
        if (num > 10000) {
          return Promise.reject(
            new Error(
              "Shard did not come into sync after " +
                num +
                " tries. " +
                newLeader
            )
          );
        }
        return rp({
          url:
            instanceManager.getEndpointUrl() +
            "/_db/_system/_admin/cluster/shardDistribution",
          json: true
        })
          .then(shardDistribution => {
            let currentShards =
              shardDistribution.results.testcollection.Current;
            let shardKey = Object.keys(currentShards)[0];
            // upon leader resign this might be undefined
            let currentServers = [];
            if (currentShards[shardKey].leader) {
              currentServers.push(currentShards[shardKey].leader);
            }

            let plannedShards = shardDistribution.results.testcollection.Plan;
            let plannedServers = [];
            if (plannedShards[shardKey].leader) {
              plannedServers.push(plannedShards[shardKey].leader);
            }
            return [
              plannedServers.concat(plannedShards[shardKey].followers),
              currentServers.concat(currentShards[shardKey].followers)
            ];
          })
          .then(result => {
            let planned = result[0];
            let current = result[1];

            return (
              current.length == numServers &&
              current.length == planned.length &&
              current[0] === newLeader &&
              current.every((server, index) => planned[index] == server)
            );
          })
          .then(finished => {
            if (!finished) {
              return new Promise((resolve, reject) => {
                setTimeout(resolve, 100);
              }).then(() => {
                return waitShardMoved(newLeader, numServers, num + 1);
              });
            }
          });
      };

      return rp({
        url:
          instanceManager.getEndpointUrl() +
          "/_db/_system/_admin/cluster/shardDistribution",
        json: true
      })
        .then(shardDistribution => {
          let shards = shardDistribution.results.testcollection.Plan;
          let shardKey = Object.keys(shards)[0];
          let is = [shards[shardKey].leader].concat(shards[shardKey].followers);
          let freeServer = servers.filter(
            server => is.indexOf(server.name) == -1
          )[0];

          if (freeServer === undefined) {
            throw new Error(
              "Don't have a free server! Servers: " +
                JSON.stringify(servers) +
                ", current: " +
                JSON.stringify(is)
            );
          }
          let should = is.slice();
          should[0] = freeServer.name;
          let move = {
            collection: "testcollection",
            database: "_system",
            shard: shardKey,
            fromServer: servers.filter(
              server => server.name == shards[shardKey].leader
            )[0].id,
            toServer: freeServer.id
          };
          return rp({
            url:
              instanceManager.getEndpointUrl() +
              "/_db/_system/_admin/cluster/moveShard",
            json: true,
            body: move,
            method: "POST"
          }).then(() => {
            return should;
          });
        })
        .then(should => {
          return waitShardMoved(should[0], should.length, 1);
        })
        .then(() => {
          return moveShards();
        });
    };

    let insertDocuments = function() {
      return [...Array(10000).keys()].reduce((promise, key) => {
        return promise.then(() => {
          return db.collection("testcollection").save({ hallooo: key });
        });
      }, Promise.resolve());
    };

    let movePromise = moveShards();

    return insertDocuments()
      .then(() => {
        console.log("Done inserting 10000 docs.");
        moveShards.stop = true;
        return movePromise;
      })
      .then(() => {
        console.log("movePromise resolved");
        return db.collection("testcollection").count();
      })
      .then(count => {
        return db.collection("testcollection").all();
      })
      .then(cursor => {
        return cursor.all();
      })
      .then(all => {
        all = new Set(all.map(doc => doc.hallooo));

        let errorMsg = "";
        if (all.size !== 10000) {
          for (let i = 0; i < 10000; ++i) {
            if (!all.has(i)) {
              errorMsg += `Document ${i} missing!\n`;
            }
          }
          // If you get the following error message, most probably something is
          // wrong in the test code.
          for (const i of all) {
            if (!_.inRange(i, 0, 10000)) {
              errorMsg += `Unexpected document ${i}!\n`;
            }
          }
        }
        expect(all.size, errorMsg).to.equal(10000);
      });
  });
});
