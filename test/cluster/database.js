/* global describe, it, before, after */
"use strict";
const InstanceManager = require("../../InstanceManager.js");
const expect = require("chai").expect;
const arangojs = require("arangojs");
const {sleep, afterEachCleanup} = require('../../utils');
const rp = require("request-promise-native");
const endpointToUrl = InstanceManager.endpointToUrl;

const databaseName = "myDatabase";
const maxRetries = 20;

const expectedSystemCollections = [
  "_analyzers",
  "_appbundles",
  "_apps",
  "_aqlfunctions",
  "_fishbowl",
  "_graphs",
  "_jobs",
  "_queues"
];


describe("Database", function() {
  const instanceManager = InstanceManager.create();
  let db;

  let debugSetFailAt = async function (point, instance) {
    await rp({
      method: "PUT",
      url: endpointToUrl(instance.endpoint) + "/_admin/debug/failat/" + encodeURIComponent(point),
      json: true,
      body: "",
      followRedirects: false
    });
  }

  const checkIfDbExistsOnCoordinator = async function (instance, database) {
    db = arangojs({
      url: instanceManager.getEndpointUrl(instance),
      databaseName: "_system"
    });
    let names = await db.listDatabases();
    return names.includes(database);
  }

  // const maxAgencyRetries = 2 * 60 * 10; // 10 minutes
  const maxAgencyRetries = 2 * 60 * 3; // 3 minutes
  const agencyRetryTime = 500;

  const readDatabaseFromAgency = async function (database) {
    for (let i = 0; i < maxAgencyRetries; i++) {
      const [info] = await InstanceManager.rpAgency({
        method: "POST",
        uri: endpointToUrl(instanceManager.getAgencyEndpoint()) + "/_api/agency/read",
        json: true,
        body: [["/"]],
      });

      if (info.arango.Plan.Databases[database]) {
        return true;
      }

      await sleep(agencyRetryTime);
    }
    return false;
  }

  const waitForDatabaseRemovalFromAgency = async function (database) {
    let gotRemoved = false;

    for (let i = 0; i < maxAgencyRetries; i++) {
      const [info] = await InstanceManager.rpAgency({
        method: "POST",
        uri: endpointToUrl(instanceManager.getAgencyEndpoint()) + "/_api/agency/read",
        json: true,
        body: [["/"]],
      });

      if (!info.arango.Plan.Databases[database] || info.arango.Plan.Databases[database] === undefined) {
        gotRemoved = true;
        return gotRemoved;
      }

      await sleep(agencyRetryTime);
    }
    return gotRemoved;
  }

  beforeEach(async function() {
    // start 1 agent
    // start 3 coordinators
    // start 5 dbservers
    await instanceManager.startCluster(1, 3, 3);
    
    db = arangojs({
      url: instanceManager.getEndpointUrl(),
      databaseName: "_system"
    });
  });

/*
 *  Actual test case section
 */

  it("create a database and forcefully reboot the coordinator during creation", async function() {
    // maintenance job should clean up agency plan
    // make sure to use exactly the same coordinator
    const coordinatorToReboot = instanceManager.coordinators()[0];
    const coordinatorToVerifyA = instanceManager.coordinators()[1];
    const coordinatorToVerifyB = instanceManager.coordinators()[2];

    db = arangojs({
      url: instanceManager.getEndpointUrl(coordinatorToReboot),
      databaseName: "_system"
    });

    // 5 seconds internal coordinator sleep
    await debugSetFailAt('UpgradeTasks::HideDatabaseUntilCreationIsFinished', coordinatorToReboot);

    // create a new database (async, we do not care about the result)
    (async function() {
      try {
        db.createDatabase(databaseName);
      } catch (err) {
        console.log(new Date().toISOString() + " " + err);
      }
    })();

    console.log(new Date().toISOString() + " Try to read database entry in agency...");

    // check agency
    let found = await readDatabaseFromAgency(databaseName);
    if (!found) {
      expect(true).to.be.false; // We did not find the entry in the agency, something is wrong
    }

    await instanceManager.shutdown(coordinatorToReboot);
    await instanceManager.restart(coordinatorToReboot);

    console.log(new Date().toISOString() + " Try to find the database on alive coordinators...");

    // databases are not allowed to show up in the (alive) coordinators 
    let checkA = await checkIfDbExistsOnCoordinator(coordinatorToVerifyA, databaseName);
    let checkB = await checkIfDbExistsOnCoordinator(coordinatorToVerifyB, databaseName);
    expect(checkA).to.be.false;
    expect(checkB).to.be.false;

    let removed = await waitForDatabaseRemovalFromAgency(databaseName);
    // expect that the agency entry got cleaned up via the supervision job (after some time)
    expect(removed).to.be.true;
  });

  it("create a database and kill the coordinator during creation", async function() {
    // maintenance job should clean up agency plan
    // make sure to use exactly the same coordinator
    const coordinatorToKill = instanceManager.coordinators()[0];
    const coordinatorToVerifyA = instanceManager.coordinators()[1];
    const coordinatorToVerifyB = instanceManager.coordinators()[2];

    db = arangojs({
      url: instanceManager.getEndpointUrl(coordinatorToKill),
      databaseName: "_system"
    });

    // active failure point - this one will FATAL_EXIT the coordinator
    // we will now kill the coordinator during that wait and have a new db in the plan
    // with the state "isBuilding". This agency plan entry should be cleaned up automatically
    // by the maintenance job afterwards.
    // instanceManager.kill(coordinatorToKill); - not needed anymore, fatal exit internally
    await debugSetFailAt('UpgradeTasks::FatalExitDuringDatabaseCreation', coordinatorToKill);

    // create a new database (async, we do not care about the result)
    (async function() {
      try {
        db.createDatabase(databaseName);
      } catch (err) {
        console.log(err);
      }
    })();

    // check agency
    let found = await readDatabaseFromAgency(databaseName);
    if (!found) {
      expect(true).to.be.false; // We did not found the entry in the agency, something is wrong
    }

    // databases are not allowed to show up in the (alive) coordinators 
    let checkA = await checkIfDbExistsOnCoordinator(coordinatorToVerifyA, databaseName);
    let checkB = await checkIfDbExistsOnCoordinator(coordinatorToVerifyB, databaseName);
    expect(checkA).to.be.false;
    expect(checkB).to.be.false;

    let removed = await waitForDatabaseRemovalFromAgency(databaseName);
    // expect that the agency entry got cleaned up via the supervision job (after some time)
    expect(removed).to.be.true;
  });

  afterEach(() => afterEachCleanup(this, instanceManager));
});
