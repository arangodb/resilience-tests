/* global describe, it, afterEach */
"use strict";

const InstanceManager = require("../../InstanceManager.js");
const endpointToUrl = InstanceManager.endpointToUrl;
const {sleep, debugLog} = require('../../utils');

const rp = require("request-promise-native");
const arangojs = require("arangojs");
const expect = require("chai").expect;
const _ = require('lodash');

// <copied from agency.js>
const agencyRequest = async function(options) {
  options.followAllRedirects = true;

  for (let tries = 0; tries < 5; tries++) {
    try {
      return await rp(options);
    } catch (err) {
      if (err.statusCode === 503 && err.message === "No leader") {
        // This may happen, just retry
      }
      else {
        // Abort when encountering other errors
        throw err;
      }
    }
  }

  throw new Error("no leader after 5 retries");
};

const waitForLeader = async function(agents) {
  const isNonEmptyString = x => _.isString(x) && x !== '';

  for (
    const start = Date.now();
    Date.now() - start < 30e3;
    await sleep(100)
  ) {
    try {
      const result = await rp({
        url: endpointToUrl(agents[0].endpoint) + "/_api/agency/config",
        json: true
      });
      if (isNonEmptyString(result.leaderId)) {
        return result.leaderId;
      }
    } catch(err) {
    }
  }
};

const getLeaderInstance = async function(agents) {
  debugLog("getting leader instance");
  const leaderId = await waitForLeader(agents);
  const getAgencyConfig = agent => agencyRequest({
    url: endpointToUrl(agent.endpoint) + "/_api/agency/config",
    json: true
  });

  for (const agent of agents) {
    const result = await getAgencyConfig(agent);
    if (result.configuration.id === leaderId) {
      return agent;
    }
  }
};
// </copied from agency.js>

describe("Leader-Follower failover + agency outage", async function() {
  const instanceManager = InstanceManager.create();
  let agents = [];

  // start a resilient agency
  beforeEach(async function() {
    agents = await instanceManager.startAgency({ agencySize: 3 });
  });

  afterEach(async function() {
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    const retainDir = currentTest.state === "failed";
    instanceManager.moveServerLogs(this.currentTest);
    try {
      await instanceManager.cleanup(retainDir);
    } catch(e) {
    }
  });

  // Actual data synchronization
  describe("with data transfer", async function() {
    async function generateData(db, num) {
      const coll = await db.collection("testcollection");
      await coll.create();
      return Promise.all(
        Array.apply(0, Array(num))
          .map((x, i) => i)
          .map(i => coll.save({ test: i }))
      );
    }

    async function checkData(db, num) {
      const cursor = await db.query(`FOR x IN testcollection
                                    SORT x.test ASC RETURN x`);
      expect(cursor.hasNext()).to.equal(true);
      let i = 0;
      while (cursor.hasNext()) {
        const doc = await cursor.next();
        expect(doc.test).to.equal(i++);
      }
      expect(i).to.equal(num);
    }

    const numDocs = 10000;
    [2, 4].forEach(n => {
      let f = n / 2;
      it(`with ${n} servers, ${f} failover, server is restarted`, async function() {
        await instanceManager.startSingleServer("single", n);
        await instanceManager.waitForAllInstances();

        // wait for leader selection
        let uuid = await instanceManager.asyncReplicationLeaderSelected();
        let leader = await instanceManager.asyncReplicationLeaderInstance();

        let db = arangojs({
          url: endpointToUrl(leader.endpoint),
          databaseName: "_system"
        });
        await generateData(db, numDocs);

        for (; f > 0; f--) {
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

          const leadAgent = await getLeaderInstance(agents);

          debugLog("killing leader %s", leader.endpoint);
          await Promise.all([
            instanceManager.kill(leader),
            instanceManager.kill(leadAgent)
          ]);
          const old = leader;

          // use a 60s timeout for selecting a new leader
          uuid = await instanceManager.asyncReplicationLeaderSelected(uuid, 60);
          leader = await instanceManager.asyncReplicationLeaderInstance();

          db = arangojs({
            url: endpointToUrl(leader.endpoint),
            databaseName: "_system"
          });
          await checkData(db, numDocs);

          await Promise.all([
            instanceManager.restart(old),
            instanceManager.restart(leadAgent)
          ]);

          debugLog("killed instances restarted");
        }
      });
    });
  });
});
