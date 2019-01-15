/* global describe, it, afterEach */
"use strict";

const InstanceManager = require("../../InstanceManager.js");
const endpointToUrl = InstanceManager.endpointToUrl;
const {sleep, debugLog, afterEachCleanup} = require('../../utils');

const arangojs = require("arangojs");
const expect = require("chai").expect;

describe("Temporary stopping", async function() {
  const instanceManager = InstanceManager.create();

  beforeEach(async function() {
    await instanceManager.startAgency({ agencySize: 1 });
  });

  afterEach(() => afterEachCleanup(this, instanceManager));

  async function generateData(db, num) {
    const coll = await db.collection("testcollection");
    let cc = 0;
    try {
      await coll.get();
      // collection exists
      const data = await coll.count();
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
    const cursor = await db.query(`FOR x IN testcollection
                                  SORT x.test ASC RETURN x`);
    expect(cursor.hasNext()).to.equal(true);
    let i = 0;
    while (cursor.hasNext()) {
      const doc = await cursor.next();
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

          debugLog("Waiting for tick synchronization...");
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

          debugLog("stopping leader %s", leader.endpoint);
          instanceManager.sigstop(leader);
          const old = leader;

          // wait for a new leader
          uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
          leader = await instanceManager.asyncReplicationLeaderInstance();

          instanceManager.sigcontinue(old);
          debugLog("stopped instance continued");

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
    const n = 4;
    let f = 2;
    const numDocs = 2500;
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

      debugLog("Waiting for tick synchronization...");
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
      const follower = followers[i];
      debugLog("stopping follower %s", follower.endpoint);
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
      debugLog("stopped follower instance continued");
    }
  });

  // simulating *short* temporary hang / network error / delay
  it(`leader for a *short* time twice, should not failover`, async function() {
    const n = 4;
    let f = 2;
    const numDocs = 2500;
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

      debugLog("Waiting for tick synchronization...");
      let inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
      expect(inSync).to.equal(
        true,
        "followers did not get in sync before timeout"
      );

      // leader should not change
      expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);

      debugLog("stopping leader %s", leader.endpoint);
      instanceManager.sigstop(leader);
      await sleep(1000); // simulate a short network hickup
      instanceManager.sigcontinue(leader);
      debugLog("stopped leader instance continued");

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
