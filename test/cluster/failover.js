/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Arango error code for "shutdown in progress"
const ERROR_SHUTTING_DOWN = 30;


describe("Failover", function() {
  const instanceManager = InstanceManager.create();
  let db;

  const getLeader = async function() {
    const data = await InstanceManager
      .rpAgency({
        method: "POST",
        url:
          instanceManager.getEndpointUrl(instanceManager.agents()[0]) +
          "/_api/agency/read",
        json: true,
        body: [["/"]]
      });

    const plan = data[0].arango.Plan;

    const plannedCollection = Object.values(plan.Collections["_system"])
      .find(col => col.name === "testcollection");
    const shardName = Object.keys(plannedCollection.shards)[0];
    const leaderId = plannedCollection.shards[shardName][0];
    const leaderEndpoint =
      data[0].arango.Current.ServersRegistered[leaderId].endpoint;
    return instanceManager
      .dbServers()
      .find(server => server.endpoint === leaderEndpoint);
  };

  beforeEach(async function() {
    await instanceManager.startCluster(1, 2, 2);
    db = arangojs({
      url: instanceManager.getEndpointUrl(),
      databaseName: "_system"
    });
    await db
      .collection("testcollection")
      .create({ shards: 4, replicationFactor: 2 });

    return Promise.all([
      db.collection("testcollection").save({ testung: Date.now() }),
      db.collection("testcollection").save({ testung: Date.now() }),
      db.collection("testcollection").save({ testung: Date.now() }),
      db.collection("testcollection").save({ testung: Date.now() }),
      db.collection("testcollection").save({ testung: Date.now() }),
      db.collection("testcollection").save({ testung: Date.now() }),
      db.collection("testcollection").save({ testung: Date.now() }),
    ]);
  });

  it("should fail over to another replica when a server goes down", async function() {
    const dbServer = await getLeader();
    await instanceManager.shutdown(dbServer);
    await db.collection("testcollection").save({ testung: Date.now() });
    const count = await db.collection("testcollection").count();
    expect(count.count).to.equal(8);
  });

  it("should allow importing even when a leader fails", async function() {
    const docs = [...Array(10000)]
      .map((_, key) => ({
        _key: "k" + key,
        hans: "kanns"
      }));
    const dbServer = await getLeader();

    const slicedImportNew = async function() {
      const count = 10;
      let inRetry = false;
      let failures = 0;

      for (let index = 0; index < docs.length; index += count) {
        try {
          await db
            .collection("testcollection")
            .import(docs.slice(index, index + count));
          inRetry = false;
          await sleep(100);
        } catch (reason) {
          if (inRetry) {
            // In this phase the server is performing the failover.
            // Until the failover is recognized Arango will forward the
            // SHUTDOWN message. So we wait a little while
            // and try again until SHUTDOWN is gone.
            // Then failover should be performed
            expect(reason.errorNum).to.equal(ERROR_SHUTTING_DOWN);
            // As we get here only for the second request fail
            // it is expected that the leader is not reachable
            await sleep (100);
            // => It is safe to assume that the import did
            // not work at all. So try again with the same slice
            index -= count;
          } else {
            // Ok first error during import.
            // The failover has just begun...
            failures++;
            inRetry = true;
            // It is undefined what happened to this slice
            // it may either be inserted or not, depending
            // on the timestamp where the server shut down.
          }
        }
      }

      return failures;
    };

    const [failures] = await Promise.all([
      slicedImportNew(),
      instanceManager.shutdown(dbServer)
    ]);

    const count = await db.collection("testcollection").count();
    expect(count.count).to.be.at.least(10007 - 10 * failures);
    expect(count.count).to.be.at.most(10007);
    expect(failures).to.be.at.most(1);
    const cursor = await db.collection("testcollection").all();
    const savedDocs = await cursor.all();
    expect(savedDocs.length).to.be.at.least(10007 - 10 * failures);
    expect(savedDocs.length).to.be.at.most(10007);
  });

  afterEach(async function() {
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    const retainDir = currentTest.state === "failed";
    await instanceManager.cleanup(retainDir);
  });
});
