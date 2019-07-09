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

  const getLeaderOrFollower = async function(follower) {
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
    let dbServerId;
    if (follower) {
      // get first follower
      dbServerId = plannedCollection.shards[shardName][1];
    } else {
      // set the leader
      dbServerId = plannedCollection.shards[shardName][0];
    }
    let dbServerEndpoint;
    try {
      dbServerEndpoint = data[0].arango.Current.ServersRegistered[dbServerId].endpoint;

    } catch (ignore) {
      dbServerEndpoint = undefined;
    }
    
    return instanceManager
      .dbServers()
      .find(server => server.endpoint === dbServerEndpoint);
  };

  async function createOneMillionDocs() {
    console.log("Create 1 mio docs");
    // TODO: change to 1mio finally, when testing done
    const amount = 100;
    for (let i = 0; i < amount; i++) {
      db.collection(replicationCollectionName).save({ testung: Date.now() });
    }
    console.log("Docs created");
  }

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

    // TODO: write 1mio docs
    return Promise.all([
      createOneMillionDocs(),
    ]);
  });

  let maxRetries = 20;
  let offlineFollowers = [];
  let leaderName;

  async function shutdownFollower() {
    if (!leaderName) {
      const leader = await getLeaderOrFollower();
      leaderName = leader.name;
    }
    let dbServers = await instanceManager.dbServers()
    let serverToShutdown;
    console.log("Leader name is: " + leaderName);

    var found = false;
    dbServers.forEach(function(dbServer) {
      if (dbServer.name !== leaderName) {
        let follower = dbServer;
        if (offlineFollowers.indexOf(dbServer.name) === -1 && !found) {
          // not found, so add to list 
          serverToShutdown = dbServer;
          found = true;
          console.log(offlineFollowers);
        }
      }
    });

    if (serverToShutdown) {
      console.log("Shut down : " + serverToShutdown.name);
      await instanceManager.shutdown(serverToShutdown);
      offlineFollowers.push(serverToShutdown.name);
    }

  }

  async function shutdownDBServerAndWaitForLeader(follower) {
    const dbServer = await getLeaderOrFollower(follower);
    let oldDBServerName = dbServer.name;
    await instanceManager.shutdown(dbServer);

    let newLeaderSelected = false;

    // then refetch leader state every second 
    for (let run = 1; run <= maxRetries; run++ ) {
      let newDBServer = await getLeaderOrFollower();
      
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
 
  it("collection should be set to read-only mode after nr of dbservers drops below minReplicationFactor - always dropping first found follower", async function() {
    // 5 DBServers available, write to a collection should work flawlessly.
    let c1 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    await expect(c1).of.be.not.null;
    await shutdownFollower();

    // 4 of 5 DBServers left, write to a collection should work flawlessly.
    let c2 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c2).of.be.not.null;
    await shutdownFollower();
    
    // 3 of 5 DBServers left, write to a collection should work flawlessly.
    let c3 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c3).of.be.not.null;
    await shutdownFollower();
    
    // 2 of 5 DBServers left, write to a collection should fail. Collection is now in read-only mode.
    try {
      let testX = await db.collection(replicationCollectionName).save({ testung: Date.now() });
      console.log(testX);
      expect.fail();
    } catch (e) {
      console.log(e);
      expect(e.isArangoError).to.be.true;
      expect(e.errorNum).to.equal(1004);
      expect(e.statusCode).to.equal(403);
    }
    await shutdownFollower();
    
    // 1 of 5 DBServers left, write to a collection should fail. Collection is now in read-only mode.
    try {
      let testY = await db.collection(replicationCollectionName).save({ testung: Date.now() });
      console.log(testY);
      expect.fail();
    } catch (e) {
      console.log(e);
      expect(e.isArangoError).to.be.true;
      expect(e.errorNum).to.equal(1004);
      expect(e.statusCode).to.equal(403);
    }
    
    const count = await db.collection(replicationCollectionName).count();
    expect(count.count).to.equal(1000003);
  });

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
    expect(count.count).to.equal(1000003);
  });

  afterEach(() => afterEachCleanup(this, instanceManager));
});
