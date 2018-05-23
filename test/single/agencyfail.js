/* global describe, it, afterEach */
"use strict";

const InstanceManager = require("../../InstanceManager.js");
const endpointToUrl = InstanceManager.endpointToUrl;

const rp = require("request-promise");
const arangojs = require("arangojs");
const expect = require("chai").expect;

// <copied from agency.js>
let agencyRequest = function(options) {
  options.followRedirects = options.followRedirects || false;
  return rp(options)
    .then(response => {
      return response;
    })
    .catch(err => {
      if (err.statusCode === 307) {
        options.url = err.response.headers["location"];
        return agencyRequest(options);
      } else if (err.statusCode === 503 && err.message === "No leader") {
        options.retries = (options.retries || 0) + 1;
        if (options.retries < 5) {
          return agencyRequest(options);
        }
        return Promise.reject(new Error("no leader after 5 retries"));
      }
      return Promise.reject(err);
    });
};

let waitForLeader = function(agents) {
  return rp({
    url: endpointToUrl(agents[0].endpoint) + "/_api/agency/config",
    json: true
  })
    .then(result => {
      if (result.leaderId === "") {
        return Promise.reject(new Error("no leader"));
      }
      return result.leaderId;
    })
    .catch(() => {
      return new Promise((resolve, reject) => {
        setTimeout(resolve, 100);
      }).then(() => {
        return waitForLeader(agents);
      });
    });
};

let getLeadAgentInstance = function(agents) {
  return waitForLeader(agents).then(leaderId => {
    return agents.reduce((leaderInstance, agent) => {
      if (leaderInstance) {
        return leaderInstance;
      }

      return agencyRequest({
        url: agent.endpoint + "/_api/agency/config",
        json: true
      }).then(result => {
        if (result.configuration.id === leaderId) {
          return agent;
        }
      });
    });
  });
};
// </copied from agency.js>

describe("Leader-Follower failover + agency outage", async function() {
  const instanceManager = InstanceManager.create();
  let agents = [];

  // start a resilient agency
  beforeEach(async function() {
    agents = await instanceManager.startAgency({ agencySize: 3 });
  });

  afterEach(function() {
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup().catch(() => {});
  });

  // Actual data synchronization
  describe("with data transfer", async function() {
    async function generateData(db, num) {
      let coll = await db.collection("testcollection");
      await coll.create();
      return Promise.all(
        Array.apply(0, Array(num))
          .map((x, i) => i)
          .map(i => coll.save({ test: i }))
      );
    }

    async function checkData(db, num) {
      let cursor = await db.query(`FOR x IN testcollection
                                    SORT x.test ASC RETURN x`);
      expect(cursor.hasNext()).to.equal(true);
      let i = 0;
      while (cursor.hasNext()) {
        let doc = await cursor.next();
        expect(doc.test).to.equal(i++);
      }
      expect(i).to.equal(num);
    }

    let numDocs = 10000;
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

          let leadAgent = await getLeadAgentInstance(agents);

          console.log("killing leader %s", leader.endpoint);
          await Promise.all([
            instanceManager.kill(leader),
            instanceManager.kill(leadAgent)
          ]);
          let old = leader;

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

          console.log("killed instances restarted");
        }
      });
    });
  });
});
