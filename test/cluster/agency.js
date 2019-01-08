/* global describe, it, beforeEach, afterEach */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const endpointToUrl = InstanceManager.endpointToUrl;

const expect = require("chai").expect;
const rp = require("request-promise-native");
const _ = require('lodash');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const debugLog = (...args) => {
  if (process.env.LOG_IMMEDIATE === "1") {
    console.log(new Date().toISOString(), ' ', ...args);
  }
};

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

const writeData = function(leader, data) {
  return agencyRequest({
    method: "POST",
    url: endpointToUrl(leader.endpoint) + "/_api/agency/write",
    json: true,
    body: data,
    followRedirects: false
  });
};

const waitForReintegration = async (endpoint) => {
  // TODO maybe reduce timeout
  const timeout = 120e3; // 120.000 ms
  let readSucceeded = false;

  const startTime = Date.now();
  while(!readSucceeded) {
    if (startTime + timeout < Date.now()) {
      throw new Error('Timeout when waiting for reintegration of '
        + endpoint);
    }

    try {
      await rp({
        method: "POST",
        url: endpointToUrl(endpoint) + "/_api/agency/read",
        json: true,
        body: [["/"]],
        followRedirects: false
      });
      readSucceeded = true;
    } catch(err) {
      if (err.statusCode === 307) {
        readSucceeded = true;
      } else {
        await sleep(100); // 100 ms
      }
    }
  }
};

const waitForLeaderChange = async function(oldLeaderEndpoint, followerEndpoint) {
  const isNonEmptyString = x => _.isString(x) && x !== '';

  for (
    const start = Date.now();
    Date.now() - start < 30e3;
    await sleep(50)
  ) {
    const res = await rp({
      url: endpointToUrl(followerEndpoint) + "/_api/agency/config",
      json: true
    });

    if (isNonEmptyString(res.leaderId)) {
      const currentLeaderEndpoint = res.configuration.pool[res.leaderId];
      if (currentLeaderEndpoint !== oldLeaderEndpoint) {
        debugLog(`currentLeaderEndpoint = ${currentLeaderEndpoint}, oldLeaderEndpoint = ${oldLeaderEndpoint}`);
        return;
      }
    }
  }

  throw new Error(`Didn't find an updated leader after 30s`);
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

  return agents
    .find(async agent => {
      const result = await getAgencyConfig(agent);
      return result.configuration.id === leaderId;
    });
};

describe("Agency", function() {
  const instanceManager = InstanceManager.create();
  let leader;
  let followers;

  describe("Agency startup", function() {
    const checkDataLoss = async function(agentCount) {
      const agents = await instanceManager
        .startAgency({ agencySize: agentCount, agencyWaitForSync: true });
      debugLog("successfully started agents");
      const data = [[{ hans: "wurst" }]];
      let leaderInstance = await getLeaderInstance(agents);
      debugLog("we got a leader");
      await writeData(leaderInstance, data);
      let result = await agencyRequest({
        method: "POST",
        url:
          endpointToUrl(leaderInstance.endpoint) + "/_api/agency/read",
        json: true,
        body: [["/"]]
      });
      expect(result).to.be.instanceof(Array);
      expect(result).to.eql(data[0]);
      debugLog("shutdown agents");
      await Promise.all(
        agents.map(agent => instanceManager.shutdown(agent))
      );
      debugLog("reboot agents");
      await Promise.all(
        agents.map(agent => instanceManager.restart(agent))
      );
      debugLog("reboot done");
      leaderInstance = await getLeaderInstance(agents);
      result = await agencyRequest({
        method: "POST",
        url:
          endpointToUrl(leaderInstance.endpoint) + "/_api/agency/read",
        json: true,
        body: [["/"]]
      });
      debugLog("agency did respond");
      expect(result).to.be.instanceof(Array);
      expect(result).to.eql(data[0]);
    };

    it("should not lose data upon restart when started in resilient mode", function() {
      return checkDataLoss(3);
    });

    it("should not lose data upon restart when started in single mode", function() {
      return checkDataLoss(1);
    });

    afterEach(async function() {
      const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
      const testFailed = currentTest.state === "failed";
      const retainDir = testFailed;
      const log = await instanceManager.cleanup(retainDir);
      if (testFailed) {
        this.currentTest.err.message =
          log + "\n\n" + this.currentTest.err.message;
      }
    });
  });

  describe("Agency checks", function() {
    beforeEach(async function() {
      // mop: without wait for sync we cannot trust the agency when it said it wrote everything
      // and we are doing tests to verify this behaviour here
      const agents = await instanceManager
        .startAgency({ agencySize: 3, agencyWaitForSync: true });
      leader = await getLeaderInstance(agents);
      followers = instanceManager
        .agents()
        .filter(agent => agent !== leader);
    });

    afterEach(async function() {
      const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
      const testFailed = currentTest.state === "failed";
      const retainDir = testFailed;
      const log = await instanceManager.cleanup(retainDir);
      if (testFailed) {
        this.currentTest.err.message =
          log + "\n\n" + this.currentTest.err.message;
      }
    });

    it("should failover when stopping the leader", async function() {
      const data = [[{ hans: "wurst" }]];
      await writeData(leader, data);
      await instanceManager.shutdown(leader);
      await waitForLeaderChange(leader.endpoint, followers[0].endpoint);
      const result = await agencyRequest({
        method: "POST",
        url: endpointToUrl(followers[0].endpoint) + "/_api/agency/read",
        json: true,
        body: [["/hans"]]
      });
      expect(result).to.be.instanceof(Array);
      expect(result).to.eql(data[0]);
    });

    it("should not think it is the leader after a restart", async function() {
      const data = [[{ hans: "wurst" }]];
      await writeData(leader, data);
      await instanceManager.shutdown(leader);
    // the default max RAFT timeout is 5s. after this, the agency should
    // have started a new election.
    // TODO maybe its better to wait until the agency has a new leader?
      await sleep(5e3);
      await instanceManager.restart(leader);
      const upButNotLeader = async function() {
        let leaderUnavailable = true;
        let lastError = null;
        while(leaderUnavailable) {
          try {
            await rp({
              method: "POST",
              url: endpointToUrl(leader.endpoint) + "/_api/agency/read",
              json: true,
              body: [["/"]],
              followRedirects: false
            });
            lastError = null;
          } catch(err) {
            // retry immediately...
            // we want to find errors and not grant 1s grace time
            leaderUnavailable = err.statusCode === 503;
            lastError = err;
          }
        }

        if (lastError === null) {
          throw new Error(
            "It should not report success! It should block all incoming rest requests until it redetermined who the leader is. Configresults: " +
            JSON.stringify({
              leader: results[0],
              follower: results[1]
            })
          );
        }

        expect(lastError.statusCode).to.equal(307);
      };

      await upButNotLeader();
    });

    it("should reintegrate a crashed follower", async function() {
      const data = [[{ koeln: "sued" }]];
      await writeData(leader, data);
      await instanceManager.kill(followers[0]);
      await instanceManager.restart(followers[0]);
      await waitForReintegration(followers[0].endpoint);
      const result = await agencyRequest({
        method: "POST",
        url: endpointToUrl(followers[0].endpoint) + "/_api/agency/read",
        json: true,
        body: [["/"]]
      });
      expect(result).to.be.instanceof(Array);
      expect(result).to.eql(data[0]);
    });

    it("should have the correct results after a funny fail rotation", async function() {
      const retryUntilUp = async function(callback) {
        for (
          const start = Date.now();
          Date.now() - start < 30e3;
          await sleep(50)
        ) {
          try {
            return await callback();
          } catch(err) {
            if (err.code === "ECONNRESET" || err.statusCode === 503) {
              // This is fine, retry
            } else {
              throw err;
            }
          }
        }
        throw new Error("Couldn't find leader after 30s");
      };
      for (let i = 0; i < instanceManager.instances.length * 2; i++) {
          const data = [[{ subba: { op: "increment" } }, {}, "funny" + i]];
          const data2 = [[{ dummy: 1 }]];
          const instance =
            instanceManager.instances[i % instanceManager.instances.length];

          await retryUntilUp(() => writeData(instance, data2));
          await InstanceManager.rpAgency({
            method: "POST",
            url: endpointToUrl(instance.endpoint) + "/_api/agency/write",
            json: true,
            body: data
          });
          await instanceManager.shutdown(instance);
          await instanceManager.restart(instance);
      }

      const result = await retryUntilUp(() => agencyRequest({
        method: "POST",
        url: endpointToUrl(leader.endpoint) + "/_api/agency/read",
        json: true,
        body: [["/"]]
      }));
      expect(result).to.be.instanceof(Array);
      expect(result[0]).to.eql({
        subba: instanceManager.instances.length * 2 * 1,
        dummy: 1
      });
    });

    it("should reintegrate a failed follower starting with a new endpoint", async function() {
      await instanceManager.shutdown(followers[0]);
      await instanceManager.assignNewEndpoint(followers[0]);
      await instanceManager.restart(followers[0]);
      await waitForReintegration(followers[0].endpoint);
      let result = await rp({
        url:
          endpointToUrl(followers[0].endpoint) + "/_api/agency/config",
        json: true
      });
      expect(result.leaderId).to.not.be.empty;
      expect(
        result.configuration.pool[result.configuration.id]
      ).to.equal(followers[0].endpoint);
      const followerId = result.configuration.id;
      result = await rp({
        url: endpointToUrl(leader.endpoint) + "/_api/agency/config",
        json: true
      });
      expect(
        result.configuration.pool[followerId]
      ).to.equal(followers[0].endpoint);
    });

    it("should reintegrate a failed leader starting with a new endpoint", async function() {
      await instanceManager.shutdown(leader);
      await instanceManager.assignNewEndpoint(leader);
      await instanceManager.restart(leader);
      await waitForReintegration(leader.endpoint);
      const result = await rp({
        url: endpointToUrl(leader.endpoint) + "/_api/agency/config",
        json: true
      });
      expect(result.leaderId, `result is: ${JSON.stringify(result)}`).to.not.be.empty;
      expect(
        result.configuration.pool[result.configuration.id]
      ).to.equal(leader.endpoint);
      const oldLeaderId = result.configuration.id;
      // It's possible that not all followers have the config replicated
      // already. So we have to be lenient here.

      const waitForNewEndpoint = async () => {
        // TODO maybe reduce timeout
        const timeout = 120e3; // 120.000 ms
        const startTime = Date.now();

        for (let curFollower = 0; curFollower < followers.length; ) {
          const follower = followers[curFollower];
          let result;
          try {
            result = await rp({
              url: endpointToUrl(follower.endpoint) + "/_api/agency/config",
              json: true
            });
          } catch (e) {
            throw new Error(
              'Error when requesting agency config from follower '
                + JSON.stringify({
                name: follower.name,
                endpoint: follower.endpoint,
                status: follower.status,
                exitcode: follower.exitcode,
              }) + '. ' +
              'The error was ' + e + ', the response was ' + JSON.stringify(e.response)
            );
          }

          if (result.configuration.pool[oldLeaderId] === leader.endpoint) {
            // success, immediately try the next
            curFollower++;
            continue;
          }

          if (startTime + timeout < Date.now()) {
            throw new Error(
              'Timeout while waiting for all agents to see the new endpoint. ' +
              `Follower ${curFollower + 1} of ${followers.length} did not get the memo: `
              + JSON.stringify({
                name: follower.name,
                endpoint: follower.endpoint,
                status: follower.status,
                exitcode: follower.exitcode,
              }) + ' and the last response was ' + JSON.stringify(result) +
              ` where we expected .configuration.pool[${oldLeaderId}] to equal ${leader.endpoint}.`
            );
          }

          await sleep(100); // 100 ms
        }

      };

      return waitForNewEndpoint();
    });
  });
});
