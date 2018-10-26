/* global describe, it, afterEach */
"use strict";

const InstanceManager = require("../../InstanceManager.js");
const endpointToUrl = InstanceManager.endpointToUrl;

const rp = require("request-promise");
const arangojs = require("arangojs");
const expect = require("chai").expect;
const sleep = (ms = 1000) => new Promise(resolve => setTimeout(resolve, ms));

/// return the list of endpoints, in a normal cluster this is the list of
/// coordinator endpoints.
async function requestEndpoints(url) {
  url = endpointToUrl(url);
  const body = await rp.get({
    uri: `${url}/_api/cluster/endpoints`,
    json: true
  });
  if (body.error) {
    throw new Error(body);
  }
  if (!body.endpoints || body.endpoints.length == 0) {
    throw new Error(
      `AsyncReplication: not all servers ready. Have ${body.endpoints
        .length} servers`
    );
  }
  return body.endpoints;
}

describe("Temporary stopping", async function() {
  const instanceManager = InstanceManager.create();

  beforeEach(async function() {
    await instanceManager.startAgency({ agencySize: 1 });
  });

  afterEach(function() {
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    const retainDir = currentTest.state === "failed";
    instanceManager.moveServerLogs(currentTest);
    return instanceManager.cleanup(retainDir).catch(() => {});
  });

  async function generateData(db, num) {
    let coll = await db.collection("testcollection");
    let cc = 0;
    try {
      let data = await coll.get();
      // collection exists
      data = await coll.count();
      cc += data.count;
    } catch (e) {
      await coll.create();
    }
    return Promise.all(
      Array.apply(0, Array(num))
        .map((x, i) => i)
        .map(i => coll.save({ test: i + cc }))
    );
  }

  async function checkData(db, num) {
    let cursor = await db.query(`FOR x IN testcollection
                                  SORT x.test ASC RETURN x`);
    expect(cursor.hasNext()).to.equal(true);
    let i = 0;
    while (cursor.hasNext()) {
      let doc = await cursor.next();
      expect(doc.test).to.equal(i++, "unexpected document on server ");
    }
    expect(i).to.equal(num, "not all documents on server");
  }

  // sigstop master and wait for failover
  [100, 1000, 10000].forEach(numDocs => {
    [4, 6].forEach(n => {
      let f = n / 2;
      it(`single leader with ${n -
        1} followers ${f} times, ${numDocs}`, async function() {
        await instanceManager.startSingleServer("single", n);
        await instanceManager.waitForAllInstances();

        // wait for leader selection
        let uuid = await instanceManager.asyncReplicationLeaderSelected();
        let leader = await instanceManager.asyncReplicationLeaderInstance();

        let db = arangojs({
          url: endpointToUrl(leader.endpoint),
          databaseName: "_system"
        });
        let expectedNumDocs = 0;
        for (; f > 0; f--) {
          await generateData(db, numDocs);
          expectedNumDocs += numDocs;

          console.log("Waiting for tick synchronization...");
          const inSync = await instanceManager.asyncReplicationTicksInSync(
            120.0
          );
          expect(inSync).to.equal(
            true,
            "followers did not get in sync before timeout"
          );

          // leader should not change
          expect(await instanceManager.asyncReplicationLeaderId()).to.equal(
            uuid
          );

          console.log("stopping leader %s", leader.endpoint);
          instanceManager.sigstop(leader);
          let old = leader;

          // wait for a new leader
          uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
          leader = await instanceManager.asyncReplicationLeaderInstance();

          instanceManager.sigcontinue(old);
          console.log("stopped instance continued");

          db = arangojs({
            url: endpointToUrl(leader.endpoint),
            databaseName: "_system"
          });
          await checkData(db, expectedNumDocs);

          // FIXME check data on old leader in read-only mode
        }
      });
    });
  });

  // stopping a random follower
  it(`random follower, 4 servers total, stopping twice`, async function() {
    let n = 4;
    let f = 2;
    let numDocs = 2500;
    await instanceManager.startSingleServer("single", n);
    await instanceManager.waitForAllInstances();

    // wait for leader selection
    let uuid = await instanceManager.asyncReplicationLeaderSelected();
    let leader = await instanceManager.asyncReplicationLeaderInstance();

    let db = arangojs({
      url: endpointToUrl(leader.endpoint),
      databaseName: "_system"
    });
    let expectedNumDocs = 0;
    for (; f > 0; f--) {
      await generateData(db, numDocs);
      expectedNumDocs += numDocs;

      console.log("Waiting for tick synchronization...");
      let inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
      expect(inSync).to.equal(
        true,
        "followers did not get in sync before timeout"
      );

      // leader should not change
      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);

      let followers = instanceManager
        .singleServers()
        .filter(
          inst => inst.status === "RUNNING" && inst.endpoint != leader.endpoint
        );
      let i = Math.floor(Math.random() * followers.length);
      let follower = followers[i];
      console.log("stopping follower %s", follower.endpoint);
      instanceManager.sigstop(follower);

      await sleep(1000);
      // leader should not have changed
      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);
      inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
      expect(inSync).to.equal(
        true,
        "followers did not get in sync before timeout"
      );

      // check the data on the master
      db = arangojs({
        url: endpointToUrl(leader.endpoint),
        databaseName: "_system"
      });
      await checkData(db, expectedNumDocs);

      instanceManager.sigcontinue(follower);
      console.log("stopped follower instance continued");
    }
  });

  // simulating *short* temporary hang / network error / delay
  it(`leader for a *short* time twice, should not failover`, async function() {
    let n = 4;
    let f = 2;
    let numDocs = 2500;
    await instanceManager.startSingleServer("single", n);
    await instanceManager.waitForAllInstances();

    // wait for leader selection
    let uuid = await instanceManager.asyncReplicationLeaderSelected();
    let leader = await instanceManager.asyncReplicationLeaderInstance();

    let db = arangojs({
      url: endpointToUrl(leader.endpoint),
      databaseName: "_system"
    });
    let expectedNumDocs = 0;
    for (; f > 0; f--) {
      await generateData(db, numDocs);
      expectedNumDocs += numDocs;

      console.log("Waiting for tick synchronization...");
      let inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
      expect(inSync).to.equal(
        true,
        "followers did not get in sync before timeout"
      );

      // leader should not change
      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);

      console.log("stopping leader %s", leader.endpoint);
      instanceManager.sigstop(leader);
      await sleep(1000); // simulate a short network hickup
      instanceManager.sigcontinue(leader);
      console.log("stopped leader instance continued");

      // leader should not change
      await sleep(2000);
      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);
      await sleep(2000);
      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);

      inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
      expect(inSync).to.equal(
        true,
        "followers did not get in sync before timeout"
      );

      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);
    }
    await checkData(db, expectedNumDocs);
  });
});
