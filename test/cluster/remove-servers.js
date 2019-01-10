/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const rp = require("request-promise-native");
const {sleep} = require('../../utils');

describe("Remove servers", function() {
  const instanceManager = InstanceManager.create();

  before(async function() {
    // should really be implemented somewhere more central :S
    const agents = await instanceManager
      .startAgency({ agencySize: 1, agencyWaitForSync: false });
    await instanceManager.waitForAllInstances();
    const version = await rp({
      url: instanceManager.getEndpointUrl(agents[0]) + "/_api/version",
      json: true
    });
    const parts = version.version.split(".")
      .map(num => parseInt(num, 10));
    if (parts[0] < 3 || parts[1] < 2) {
      this.skip();
    }
    await instanceManager.cleanup();
  });

  const waitForHealth = async function(serverEndpoint, maxTime) {
    const coordinator = instanceManager
      .coordinators()
      .filter(server => server.status === "RUNNING")[0];
    for (
      const start = Date.now();
      Date.now() - start < maxTime;
      await sleep(100)
    ) {
      try {
        const response = await rp({
          url: instanceManager.getEndpointUrl(coordinator)
            + "/_admin/cluster/health",
          json: true
        });
        const health = response.Health;
        const serverId = Object.keys(health).filter(serverId => {
          return health[serverId].Endpoint === serverEndpoint;
        })[0];

        if (serverId !== undefined) {
          return health[serverId];
        }
      } catch (e) {
      }
    }
    throw new Error("Server did not go failed in time!");
  };

  const waitForFailedHealth = async function(serverId, maxTime) {
    const coordinator = instanceManager
      .coordinators()
      .filter(server => server.status === "RUNNING")[0];

    for (
      const start = Date.now();
      Date.now() - start < maxTime;
      await sleep(100)
    ) {
      const response = await rp({
        url: instanceManager.getEndpointUrl(coordinator)
          + "/_admin/cluster/health",
        json: true
      });
      const health = response.Health;
      if (!health.hasOwnProperty(serverId)) {
        throw new Error(`Couldn't find a server in health struct. `
          + `Looking for ${serverId}, Health = ${JSON.stringify(health)}`);
      }

      const healthServer = health[serverId];
      if (healthServer.Status === "FAILED") {
        return true;
      }
    }
    throw new Error("Server did not go failed in time!");
  };

  afterEach(async function() {
    instanceManager.moveServerLogs(this.currentTest);
    const currentTest = this.ctx ? this.ctx.currentTest : this.currentTest;
    const retainDir = currentTest.state === "failed";
    await instanceManager.cleanup(retainDir);
  });

  it("should mark a failed coordinator failed after a while", async function() {
    await instanceManager.startCluster(1, 2, 2);
    const coordinator = instanceManager.coordinators()[1];
    await instanceManager.shutdown(coordinator);
    await waitForFailedHealth(coordinator.id, Date.now() + 60000);
  });

  it("should not be possible to remove a running coordinator", async function() {
    await instanceManager.startCluster(1, 2, 2);
    const response = await rp({
      url: instanceManager.getEndpointUrl() + "/_admin/cluster/health",
      json: true
    });
    const health = response.Health;
    const serverId = Object.keys(health).filter(serverId => {
      return health[serverId].Role === "Coordinator";
    })[0];


    let exception = null;

    try {
      await rp({
        url:
          instanceManager.getEndpointUrl() + "/_admin/cluster/removeServer",
        json: true,
        method: "post",
        body: serverId
      });
    } catch (err) {
      exception = err;
    }

    if (exception === null) {
      throw new Error(
        "What? Removing a server that is active should not be possible"
      );
    }

    expect(exception.statusCode).to.eql(412);
  });

  it("should raise a proper error when removing a non existing server", async function() {
    await instanceManager.startCluster(1, 2, 2);

    let exception = null;
    try {
      await rp({
        url:
          instanceManager.getEndpointUrl() + "/_admin/cluster/removeServer",
        json: true,
        method: "post",
        body: "der hund"
      });
    } catch(err) {
      exception = err;
    }

    if (exception === null) {
      throw new Error(
        "What? Removing a non existing server should not be possible"
      );
    }

    expect(exception.statusCode).to.eql(404);
  });

  it("should be able to remove a failed coordinator", async function() {
    await instanceManager.startCluster(1, 2, 2);
    const coordinator = instanceManager.coordinators()[1];
    await instanceManager.shutdown(coordinator);
    await waitForFailedHealth(
      coordinator.id,
      Date.now() + 60000
    );

    await rp({
      url:
        instanceManager.getEndpointUrl() + "/_admin/cluster/removeServer",
      json: true,
      method: "post",
      body: coordinator.id,
    });
  });

  it("should be able to remove a failed dbserver which has no responsibilities", async function() {
    await instanceManager.startCluster(1, 2, 2);
    const dbserver = await instanceManager.startDbServer("fauler-hund");
    await instanceManager.waitForAllInstances();
    await instanceManager.addIdsToAllInstances();
    await waitForHealth(dbserver.endpoint, Date.now() + 60000);
    await instanceManager.shutdown(dbserver);
    await waitForFailedHealth(dbserver.id, Date.now() + 60000);

    await rp({
      url:
        instanceManager.getEndpointUrl() + "/_admin/cluster/removeServer",
      json: true,
      method: "post",
      body: dbserver.id,
    });
  });
});
