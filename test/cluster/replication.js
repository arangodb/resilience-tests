/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");
const {sleep, afterEachCleanup} = require('../../utils');

const replicationCollectionName = "replicationCollectionName";
let replicationCollectionId = 0;
let shardName = "_unknown_";
const maxRetries = 20;
const offlineFollowers = new Set();
let leaderName;


describe("Replication", function() {
  const instanceManager = InstanceManager.create();
  let db;

  const getMultiInfoFromAgency = async function(path) {
    // Path needs to be an array e.g. ["Plan", "Collections", "_system"]
    // Or an array of above Paths
    if (!Array.isArray(path) || !Array.isArray(path[0])) {
      throw "Paths needs to be an array of arrays. Test code wrong";
    }
    const body = path.map(p => `/arango/${p.join("/")}`);
    const data = await InstanceManager
    .rpAgency({
      method: "POST",
      url:
        instanceManager.getEndpointUrl(instanceManager.agents()[0]) +
        "/_api/agency/read",
      json: true,
      body: [body]
    });

   // Same as data[0].arango.(path.join(".")) evaluated
    return path.map(p => p.reduce((obj, attribute) => obj[attribute], data[0].arango));
  };

  const getInfoFromAgency = async function(path) {
    // Path needs to be an array e.g. ["Plan", "Collections", "_system"]
    // Or an array of above Paths
    if (!Array.isArray(path)) {
      throw "Path needs to be an array. Test code wrong";
    }
    return (await getMultiInfoFromAgency([path]))[0];
  };

  const getCurrentFailoverCandidates = async () => {
    return await getInfoFromAgency(["Current", "Collections", "_system", replicationCollectionId, shardName, "failoverCandidates"]);
  };
  
  
  const getCurrentInSyncFollowers = async () => {
    return await getInfoFromAgency(["Current", "Collections", "_system", replicationCollectionId, shardName, "servers"]);
  };
  
  const getLeaderOrFollower = async function(follower) {
    const servers = await getInfoFromAgency(["Plan", "Collections", "_system", replicationCollectionId, "shards", shardName]);
    let dbServerId;
    if (follower) {
      // get first follower
      dbServerId = servers[1];
    } else {
      // set the leader
      dbServerId = servers[0];
    }
    let dbServerEndpoint;
    try {
      dbServerEndpoint = await getInfoFromAgency(["Current","ServersRegistered", dbServerId, "endpoint"]);
    } catch (ignore) {
      dbServerEndpoint = undefined;
    }
    
    return instanceManager
      .dbServers()
      .find(server => server.endpoint === dbServerEndpoint);
  };
  
  async function createOneMillionDocs() {
    console.log("Create 1 mio docs");
    const docs = [];
    for (let i = 0; i < 10000; ++i) {
      docs.push({ testung: Date.now() });
    }
    // TODO: change to 1mio finally, when testing done
    const amount = 100;
    for (let i = 0; i < amount; i++) {
      await db.collection(replicationCollectionName).save(docs);
    }
    console.log("Docs created");
  };


  async function shutdownFollower() {
    if (!leaderName) {
      const leader = await getLeaderOrFollower();
      leaderName = leader.name;
    }
    let dbServers = await instanceManager.dbServers()
    console.log("Leader name is: " + leaderName);

    for (const dbServer of dbServers) {
      const {name} = dbServer;
      if (name !== leaderName && !offlineFollowers.has(name)) {
        console.log("Shut down : " + name);
        await instanceManager.shutdown(dbServer);
        console.log("Server : " + name + " is now offline.");
        offlineFollowers.add(name);
        return;
      }
    }
  }

  async function shutdownLeader() {
    const oldFollowers = await getCurrentInSyncFollowers();
    const dbServer = await getLeaderOrFollower();
    await instanceManager.shutdown(dbServer);
    return oldFollowers[0];
  }

  async function suspendLeader() {
    const oldFollowers = await getCurrentInSyncFollowers();
    const dbServer = await getLeaderOrFollower();
    await instanceManager.sigstop(dbServer);
    return {dbServer, name: oldFollowers[0]};
  }

  async function continueServer(dbServer) {
    return await instanceManager.sigcontinue(dbServer);
  }

  const validateInsyncAndFocInLine = async () => {
    const [foc, insync] = await getMultiInfoFromAgency([
      ["Current", "Collections", "_system", replicationCollectionId, shardName, "failoverCandidates"],
      ["Current", "Collections", "_system", replicationCollectionId, shardName, "servers"]
    ]);
    expect(insync).to.eql(foc);
  }
  
  const validateROMode = async (availableFollowers, lastLeader) => {
    let changedLeader = false;
    for (let i = 0; i < 100; ++i) {
      const candidates = await getCurrentFailoverCandidates();
       // We are NEVER allowed to get to less then minReplicationFactor candidates
      expect(candidates).to.have.length.above(2);
      try {
        // TRY to insert
        await db.collection(replicationCollectionName).save({ testung: Date.now() });
        // If we can write we must be upgraded to more than 2
        const inSync = await getCurrentInSyncFollowers();
        if (inSync[0] !== lastLeader) {
          changedLeader = true;
        }
        expect(changedLeader).to.be.true;
        if (changedLeader && inSync.length === availableFollowers) {
          await validateInsyncAndFocInLine();
          // We stop as soon as we reach the given number of availableFollowers
          // really in sync
          const foC = await getCurrentFailoverCandidates();
          expect(inSync).to.eql(foC);
          return;
        }
      } catch (e) {
        if (!e.isArangoError) {
          // Assertion!
          throw e;
        }
        expect(e.isArangoError).to.be.true;
        expect(e.errorNum).to.equal(1004);
        expect(e.statusCode).to.equal(403);
      }
      await sleep(1000);
    }
  };


/*
 *  Actual test case section
 */

  beforeEach(async function() {
    // start 5 dbservers
    await instanceManager.startCluster(1, 3, 5);
    
    db = arangojs({
      url: instanceManager.getEndpointUrl(),
      databaseName: "_system"
    });
    const col = await db
      .collection(replicationCollectionName)
      .create({ shards: 1, minReplicationFactor: 3, replicationFactor: 5 });
    replicationCollectionId = col.id;
    const shards = await getInfoFromAgency(["Plan", "Collections", "_system", replicationCollectionId, "shards"]);
    shardName = Object.keys(shards)[0];
    offlineFollowers.clear();
    return await createOneMillionDocs();
  });
 
  it("collection should be set to read-only mode after nr of dbservers drops below minReplicationFactor - always dropping first found follower", async function() {
    // 5 DBServers available, write to a collection should work flawlessly.

    expect(await getCurrentInSyncFollowers()).to.have.length(5);
    let c1 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c1).of.be.not.null;
    await shutdownFollower();
    // We report only after a write
    expect(await getCurrentFailoverCandidates()).to.have.length(5);
    expect(await getCurrentInSyncFollowers()).to.have.length(5);

    // 4 of 5 DBServers left, write to a collection should work flawlessly.
    let c2 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c2).of.be.not.null;

    // The above has triggered a drop
    expect(await getCurrentFailoverCandidates()).to.have.length(4);
    expect(await getCurrentInSyncFollowers()).to.have.length(4);

    await shutdownFollower();
    // Failover cannot have happened yet
    expect(await getCurrentFailoverCandidates()).to.have.length(4);
    expect(await getCurrentInSyncFollowers()).to.have.length(4);

    // 3 of 5 DBServers left, write to a collection should work flawlessly.
    let c3 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    expect(c3).of.be.not.null;
    // The above has triggered a drop
    expect(await getCurrentFailoverCandidates()).to.have.length(3);
    expect(await getCurrentInSyncFollowers()).to.have.length(3);

    await shutdownFollower();
    // Failover cannot have happened yet
    expect(await getCurrentFailoverCandidates()).to.have.length(3);
    expect(await getCurrentInSyncFollowers()).to.have.length(3);
    let firstInsert = false;
    // 2 of 5 DBServers left, write to a collection should fail. Collection is now in read-only mode.
    try {
      await db.collection(replicationCollectionName).save({ testung: Date.now() });
      // The above is required to trigger the failover (latest)
      // It is actually okay to NOT get here, this is a race, both cases are covered.
      expect(await getCurrentFailoverCandidates()).to.have.length(2);
      expect(await getCurrentInSyncFollowers()).to.have.length(2);
      firstInsert = true;
      await db.collection(replicationCollectionName).save({ testung: Date.now() });
      expect.fail();
    } catch (e) {
      expect(e.isArangoError).to.be.true;
      expect(e.errorNum).to.equal(1004);
      expect(e.statusCode).to.equal(403);
    }
    expect(await getCurrentFailoverCandidates()).to.have.length(2);
    expect(await getCurrentInSyncFollowers()).to.have.length(2);

    await shutdownFollower();
    // Failover cannot have happened yet
    expect(await getCurrentFailoverCandidates()).to.have.length(2);
    expect(await getCurrentInSyncFollowers()).to.have.length(2);
    
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
    if (firstInsert) {
      expect(count.count).to.equal(1000004);
    } else {
      expect(count.count).to.equal(1000003);
    }
  });

  it("collection should be set to read-only mode after nr of dbservers drops below minReplicationFactor - always dropping leader", async function() {
    // 5 DBServers available, write to a collection should work flawlessly.
    expect(await getCurrentInSyncFollowers()).to.have.length(5);
    let c1 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    await expect(c1).of.be.not.null;
    expect(await getCurrentInSyncFollowers()).to.have.length(5);
    console.log("Shutdown first leader");
    let lastLeader = await shutdownLeader();
    console.log("Shutdown first leader done " + lastLeader);
    await validateROMode(4, lastLeader);

    console.log("Shutdown second leader");
    lastLeader = await shutdownLeader();
    await validateROMode(3, lastLeader);

    console.log("Shutdown third leader (we cannot recover to write mode from now on)");
    lastLeader = await shutdownLeader();
    // We will never be able to leave the RO mode here
    // await validateROMode(2, lastLeader);
    console.log("In RO mode");
    for (let i = 0; i < 10; ++i) {
      try {
        // TRY to insert
        await db.collection(replicationCollectionName).save({ testung: Date.now() });
        expect.fail();
      } catch (e) {
        if (!e.isArangoError) {
          // Assertion!
          throw e;
        }
        expect(e.isArangoError).to.be.true;
        expect(e.errorNum).to.equal(1004);
        expect(e.statusCode).to.equal(403);
      }
      // Keep the dead leader as potential FO candidate, if it raises back from the grave
      expect(await getCurrentFailoverCandidates()).to.have.length(3);
      sleep(1000);
    }
  });

    it("collection should recover from read-only mode after nr of dbservers rises above minReplicationFactor - always dropping leader", async function() {
    // 5 DBServers available, write to a collection should work flawlessly.
    expect(await getCurrentInSyncFollowers()).to.have.length(5);
    let c1 = await db.collection(replicationCollectionName).save({ testung: Date.now() });
    await expect(c1).of.be.not.null;
    expect(await getCurrentInSyncFollowers()).to.have.length(5);
    console.log("Shutdown first leader");
    const suspendedLeader = await suspendLeader();
    let lastLeader;
    try {
      console.log(`First leader ${suspendedLeader.name} is gone`);
      await validateROMode(4, suspendedLeader.name);

      console.log("Shutdown second leader");
      lastLeader = await shutdownLeader();
      await validateROMode(3, lastLeader);
      console.log(`Second leader ${lastLeader} is gone`);

      console.log("Shutdown third leader");
      lastLeader = await shutdownLeader();
      console.log(`Third leader ${lastLeader} is gone`);
      // We will never be able to leave the RO mode here
      // await validateROMode(2, lastLeader);
      console.log("In RO mode");
      for (let i = 0; i < 10; ++i) {
        try {
          // TRY to insert
          await db.collection(replicationCollectionName).save({ testung: Date.now() });
          expect.fail();
        } catch (e) {
          if (!e.isArangoError) {
            // Assertion!
            throw e;
          }
          expect(e.isArangoError).to.be.true;
          expect(e.errorNum).to.equal(1004);
          expect(e.statusCode).to.equal(403);
        }
        expect(await getCurrentFailoverCandidates()).to.have.length(3);
        await sleep(1000);
      }
    } catch(e) {
      // We need to continue the server, otherwise instance manager does not shutdown properly.
      await continueServer(suspendedLeader.dbServer);
      throw e;
    }
    console.log(`Continue First leader ${suspendedLeader.name}`);
    await continueServer(suspendedLeader.dbServer);
    console.log(`Server ${suspendedLeader.name} back again, try to get insync`);
    // We can now leave RO mode after suspended leader is reintegrated.
    // Also validate that the suspended leader does not harm current in any way.
    await validateROMode(3, lastLeader);

  });

  afterEach(() => afterEachCleanup(this, instanceManager));
});
