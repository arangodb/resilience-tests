/* global describe, it, before, after, afterEach */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");
const dd = require('dedent');

describe.skip("ClusterResilience", function() {
  let instanceManager = InstanceManager.create();
  let db;
  let aTestFailed;
  before(function() {
    aTestFailed = false;
    return instanceManager.startCluster(3, 2, 2).then(() => {
      db = arangojs({
        url: instanceManager.getEndpointUrl(),
        databaseName: "_system"
      });
      return db
        .collection("testcollection")
        .create({ numberOfShards: 4 })
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

  afterEach(function() {
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    if (currentTest.state === "failed") {
      aTestFailed = true;
    }
    instanceManager.moveServerLogs(this.currentTest);
  });

  after(async function() {
    const retainDir = aTestFailed;

    const test = this.test;

    const notes = dd`
      Test:
        ${test.title}
      Suite:
        ${test.parent.fullTitle()}
    ` + "\n";

    try {
      await instanceManager.cleanup(retainDir, true, notes);
    } catch(e) {
      console.warn(`Ignored cleanup error: ${e}`);
    }
  });

  it("should report the same number of documents after a db server restart", function() {
    let count = 7;
    return db
      .collection("testcollection")
      .count()
      .then(realCount => {
        expect(realCount.count).to.equal(count);
      })
      .then(() => {
        let dbServer = instanceManager.dbServers()[0];
        return instanceManager.shutdown(dbServer).then(() => {
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
        return db.collection("testcollection").count();
      })
      .then(realCount => {
        expect(realCount.count).to.equal(count);
      });
  });

  it("should report the same number of documents after a coordinator restart", function() {
    let count = 7;
    return db
      .collection("testcollection")
      .count()
      .then(realCount => {
        expect(realCount.count).to.equal(count);
      })
      .then(() => {
        let server = instanceManager.coordinators()[0];
        return instanceManager.shutdown(server).then(() => {
          return server;
        });
      })
      .then(server => {
        // mop: wait a bit to possibly make the cluster go wild!
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve(server);
          }, 1000);
        });
      })
      .then(server => {
        return instanceManager.restart(server);
      })
      .then(() => {
        return db.collection("testcollection").count();
      })
      .then(realCount => {
        expect(realCount.count).to.equal(count);
      });
  });

  it("should report 503 when a required backend is not available", function() {
    let dbServer = instanceManager.dbServers()[0];
    return instanceManager
      .shutdown(dbServer)
      .then(server => {
        // mop: wait a bit to possibly make the cluster go wild!
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve(server);
          }, 1000);
        });
      })
      .then(() => {
        return db.collection("testcollection").count();
      })
      .then(
        result => {
          // mop: error must be thrown!
          return Promise.reject(
            "ArangoDB reported success even though a backend was killed?!" +
              JSON.stringify(result)
          );
        },
        err => {
          expect(err.code).to.equal(503);
          return instanceManager.restart(dbServer);
        }
      );
  });

  it("should properly shutdown when a backend has failed", function() {
    let dbServer = instanceManager.dbServers()[0];
    return instanceManager.kill(dbServer);
    // mop: afterEach should work
  });
});
