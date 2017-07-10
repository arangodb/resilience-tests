/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");
const rp = require("request-promise");
const fs = require("fs");

describe("Failover", function() {
  let instanceManager = new InstanceManager("failover");
  let db;

  let getLeader = function() {
    return instanceManager.rpAgency({
      method: "POST",
      url: instanceManager.getEndpointUrl(instanceManager.agents()[0]) +
        "/_api/agency/read",
      json: true,
      body: [["/"]]
    }).then(data => {
      let plan = data[0].arango.Plan;

      let plannedCollection = Object.keys(
        plan.Collections["_system"]
      ).reduce((result, cid) => {
        if (result) {
          return result;
        }

        if (plan.Collections["_system"][cid].name == "testcollection") {
          return plan.Collections["_system"][cid];
        }
        return undefined;
      }, undefined);
      let shardName = Object.keys(plannedCollection.shards)[0];
      let leaderId = plannedCollection.shards[shardName][0];
      let leaderEndpoint =
        data[0].arango.Current.ServersRegistered[leaderId].endpoint;
      return instanceManager.dbServers().reduce((found, server) => {
        if (found) {
          return found;
        }

        if (server.endpoint == leaderEndpoint) {
          return server;
        }

        return undefined;
      }, undefined);
    });
  };
  beforeEach(function() {
    return instanceManager.startCluster(1, 2, 2).then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: "_system"
      });
      return db
        .collection("testcollection")
        .create({ shards: 4, replicationFactor: 2 })
        .then(() => {
          return Promise.all([
            db.collection("testcollection").save({ testung: Date.now() }),
            db.collection("testcollection").save({ testung: Date.now() }),
            db.collection("testcollection").save({ testung: Date.now() }),
            db.collection("testcollection").save({ testung: Date.now() }),
            db.collection("testcollection").save({ testung: Date.now() }),
            db.collection("testcollection").save({ testung: Date.now() }),
            db.collection("testcollection").save({ testung: Date.now() })
          ]);
        });
    });
  });

  it("should fail over to another replica when a server goes down", function() {
    return getLeader()
      .then(dbServer => {
        return instanceManager.shutdown(dbServer);
      })
      .then(() => {
        return db.collection("testcollection").save({ testung: Date.now() });
      })
      .then(() => {
        return db.collection("testcollection").count();
      })
      .then(count => {
        expect(count.count).to.equal(8);
      });
  });

  it("should allow importing even when a leader fails", function() {
    let docs = [...Array(10000)].map(function(_, key) {
      return {
        _key: "k" + key,
        hans: "kanns"
      };
    });
    let failures = 0;
    return getLeader()
      .then(dbServer => {
        let slicedImport = function(index) {
          let count = 10;
          if (index < docs.length) {
            return db
              .collection("testcollection")
              .import(docs.slice(index, index + count))
              .then(result => {
                return new Promise((resolve, reject) => {
                  setTimeout(resolve, 100);
                }).then(() => {
                  return slicedImport(index + count);
                });
              })
              .catch(reason => {
                failures++;
                return slicedImport(index + count);
              });
          } else {
            return Promise.resolve();
          }
        };
        return Promise.all([slicedImport(0), instanceManager.shutdown(dbServer)]);
      })
      .then(() => {
        return db.collection("testcollection").count();
      })
      .then(count => {
        expect(count.count).to.be.least(10007 - 10 * failures);
        expect(count.count).to.be.most(10007);
        expect(failures).to.be.most(1);
      })
      .then(() => {
        return db.collection("testcollection").all();
      })
      .then(cursor => {
        return cursor.all();
      })
      .then(savedDocs => {
        expect(savedDocs.length).to.be.least(10007 - 10 * failures);
        expect(savedDocs.length).to.be.most(10007);
      });
  });

  afterEach(function() {
    return instanceManager.cleanup();
  });
});
