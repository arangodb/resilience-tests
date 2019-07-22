/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const rp = require("request-promise-native");
const {sleep, afterEachCleanup} = require('../../utils');

describe("rebootId", function() {
  const instanceManager = InstanceManager.create();

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
  const readRebootIdFromAgency = async function(server) {
    const rebootId = await getMultiInfoFromAgency([["Current", "ServersKnown", server, "rebootID"]]);
    if (isNaN(rebootId[0])) {
      throw new Error(
          "rebootId is not an integer: " + rebootId[0]
      );
    }
    return rebootId[0];
  };

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

  const rebootIdTest = async function(instance, nrestarts, method) {
    // read rebootId
    const rebootIdBefore = await readRebootIdFromAgency(instance.id);
    for (var i = 0; i < nrestarts; i++) {
      await method(instance);
      // restart calls waitForInstance
      await instanceManager.restart(instance);
    }
    const rebootIdAfter = await readRebootIdFromAgency(instance.id);

    if (rebootIdAfter <= rebootIdBefore) {
      throw new Error(
          "rebootId did not increase after instance reboot " + instance
      );
    }
  };

  afterEach(() => afterEachCleanup(this, instanceManager));

  it("should increment rebootId on an instance after rebooting it (using shutdown)", async function() {
    await instanceManager.startCluster(1, 2, 2);

    for (const x of instanceManager.coordinators()) {
      await rebootIdTest(x, 1, i => instanceManager.shutdown(i));
    }
    for (const x of instanceManager.dbServers()) {
      await rebootIdTest(x, 1, i => instanceManager.shutdown(i));
    }
  });

  it("should increment rebootId on an instance after rebooting it (using kill)", async function() {
    await instanceManager.startCluster(1, 2, 2);

    for (const x of instanceManager.coordinators()) {
      await rebootIdTest(x, 1, i => instanceManager.kill(i));
    }
    for (const x of instanceManager.dbServers()) {
      await rebootIdTest(x, 1, i => instanceManager.kill(i));
    }
  });

  it("should increment rebootId on an instance after rebooting it (using kill or shutdown)", async function() {
    await instanceManager.startCluster(1, 2, 2);

    await rebootIdTest(instanceManager.coordinators()[0], 1, i => instanceManager.kill(i));
    await rebootIdTest(instanceManager.dbServers()[0], 1, i => instanceManager.shutdown(i));
    await rebootIdTest(instanceManager.coordinators()[1], 1, i => instanceManager.shutdown(i));
    await rebootIdTest(instanceManager.dbServers()[1], 1, i => instanceManager.kill(i));
  });

  it("should increment rebootId on a coordinator after rebooting it repeatedly (using shutdown)", async function() {
    await instanceManager.startCluster(1, 2, 2);

    const coordinator = instanceManager.coordinators()[0];

    await rebootIdTest(coordinator, 5, i => instanceManager.shutdown(i));
  });

  it("should increment rebootId on a dbserver rebooting it repeatedly (using shutdown)", async function() {
    await instanceManager.startCluster(1, 2, 2);

    const dbserver = instanceManager.dbServers()[0];

    await rebootIdTest(dbserver, 5, i => instanceManager.shutdown(i));
  });
});
