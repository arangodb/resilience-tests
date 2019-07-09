/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");
const {sleep, afterEachCleanup} = require('../../utils');

const replicationCollectionName = "replicationCollectionName";

describe("Replication", function() {
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
      .find(col => col.name === replicationCollectionName);
    const shardName = Object.keys(plannedCollection.shards)[0];
    const leaderId = plannedCollection.shards[shardName][0];
    let leaderEndpoint;
    try {
      leaderEndpoint = data[0].arango.Current.ServersRegistered[leaderId].endpoint;
    } catch (ignore) {
      leaderEndpoint = undefined;
    }
    
    return instanceManager
      .dbServers()
      .find(server => server.endpoint === leaderEndpoint);
  };

  beforeEach(async function() {
    // start 5 dbservers
    await instanceManager.startCluster(1, 3, 5);
    
    db = arangojs({
      url: instanceManager.getEndpointUrl(),
      databaseName: "_system"
    });
    await db
      .collection(replicationCollectionName)
      .create({ shards: 1, minReplicationFactor: 3, replicationFactor: 5 });
  });

  let maxRetries = 20;

  async function shutdownDBServerAndWaitForLeader() {
    const dbServer = await getLeader();
    let oldDBServerName = dbServer.name;
    await instanceManager.shutdown(dbServer);

    let newLeaderSelected = false;

    // then refetch leader state every second 
    for (let run = 1; run <= maxRetries; run++ ) {
      let newDBServer = await getLeader();
      
      let newDBServerName;
      if (newDBServer != undefined) {
        newDBServerName = newDBServer.name;
      }
      
      if (newDBServerName !== undefined && oldDBServerName !== newDBServerName) {
        // new leader was found, quick exit
        newLeaderSelected = true;
        break;
      }
      await sleep(1000);
    }
    
    return newLeaderSelected;
  }
  
  it("collection should be set to read-only mode after nr of dbservers drops below minReplicationFactor - always dropping leader", async function() {
    // 5 DBServers available, write to a collection should work flawlessly.
    let c1 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    await expect(c1).of.be.not.null;
    await shutdownDBServerAndWaitForLeader();

    // 4 of 5 DBServers left, write to a collection should work flawlessly.
    let c2 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c2).of.be.not.null;
    await shutdownDBServerAndWaitForLeader();
    
    // 3 of 5 DBServers left, write to a collection should work flawlessly.
    let c3 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c3).of.be.not.null;
    await shutdownDBServerAndWaitForLeader();
    
    // 2 of 5 DBServers left, write to a collection should fail. Collection is now in read-only mode.
    try {
      await db.collection(replicationCollectionName).save({ testung: Date.now() });
      expect.fail();
    } catch (e) {
      expect(e.isArangoError).to.be.true;
      expect(e.errorNum).to.equal(1004);
      expect(e.statusCode).to.equal(403);
    }
    await shutdownDBServerAndWaitForLeader();
    
    // 1 of 5 DBServers left, write to a collection should fail. Collection is now in read-only mode.
    try {
      await db.collection(replicationCollectionName).save({ testung: Date.now() });
      expect.fail();
    } catch (e) {
      expect(e.isArangoError).to.be.true;
      expect(e.errorNum).to.equal(1004);
      expect(e.statusCode).to.equal(403);
    }
    
    const count = await db.collection(replicationCollectionName).count();
    expect(count.count).to.equal(3);
  });

  afterEach(() => afterEachCleanup(this, instanceManager));
});
