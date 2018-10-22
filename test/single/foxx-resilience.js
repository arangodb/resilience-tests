/* global describe, it, beforeEach, afterEach */
"use strict";
const join = require("path").join;
const readFileSync = require("fs").readFileSync;
const InstanceManager = require("../../InstanceManager");
const endpointToUrl = InstanceManager.endpointToUrl;
const arangojs = require("arangojs");
const aql = arangojs.aql;
const expect = require("chai").expect;
const service1 = readFileSync(
  join(__dirname, "..", "..", "fixtures", "service1.zip")
);
const service2 = readFileSync(
  join(__dirname, "..", "..", "fixtures", "service2.zip")
);
const utilService = readFileSync(
  join(__dirname, "..", "..", "fixtures", "util.zip")
);
const UTIL_MOUNT = "/util";
const SERVICE_1_CHECKSUM = "69d01a5c";
const SERVICE_1_RESULT = "service1";
const MOUNT_1 = "/resiliencetestservice1";

const sleep = (ms = 1000) => new Promise(resolve => setTimeout(resolve, ms));

describe("Foxx service", async function() {
  const instanceManager = InstanceManager.create();

  const serviceInfos = [
    {
      mount: MOUNT_1,
      service: service1,
      checksum: SERVICE_1_CHECKSUM,
      result: SERVICE_1_RESULT
    }
  ];

  beforeEach(async function() {
    await instanceManager.startAgency({ agencySize: 1 });
  });

  afterEach(function() {
    const retainDir = this.currentTest.state === "failed";
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup(retainDir).catch(() => {});
  });

  async function generateData(db, num) {
    let coll = await db.collection("testcollection");
    await coll.create();
    return Promise.all(
      Array.apply(0, Array(num))
        .map((x, i) => i)
        .map(i => coll.save({ test: i }))
    );
  }

  it("survives leader failover", async function() {
    await instanceManager.startSingleServer("single", 2);
    await instanceManager.waitForAllInstances();

    let uuid = await instanceManager.asyncReplicationLeaderSelected();
    let leader = await instanceManager.asyncReplicationLeaderInstance();

    console.log("installing foxx app on %s", leader.endpoint);
    await installUtilService(leader.endpoint);
    await installAndCheckServices({
      endpointUrl: endpointToUrl(leader.endpoint),
      serviceInfos: serviceInfos
    });

    // wait for followers to get in sync
    const inSync = await instanceManager.asyncReplicationTicksInSync(60.0);
    expect(inSync).to.equal(
      true,
      "followers did not get in sync before timeout"
    );

    console.log("killing leader %s", leader.endpoint);
    await instanceManager.kill(leader);

    uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
    leader = await instanceManager.asyncReplicationLeaderInstance();

    // just to be sure the foxx queue got a chance to run
    await sleep(3000);

    console.log("checking service on new leader %s", leader.endpoint);
    await checkAllServices({
      endpointUrl: endpointToUrl(leader.endpoint),
      serviceInfos: serviceInfos
    });
  });

  it("survives leader failover with data", async function() {
    await instanceManager.startSingleServer("single", 2);
    await instanceManager.waitForAllInstances();

    let uuid = await instanceManager.asyncReplicationLeaderSelected();
    let leader = await instanceManager.asyncReplicationLeaderInstance();

    console.log("installing foxx app on %s", leader.endpoint);
    let leaderUrl = endpointToUrl(leader.endpoint);
    await installUtilService(leader.endpoint);
    await installAndCheckServices({
      endpointUrl: leaderUrl,
      serviceInfos: serviceInfos
    });

    let db = arangojs({ url: leaderUrl, databaseName: "_system" });
    let coll = await db.collection("testcollection");
    await coll.create();
    await Promise.all(
      Array.apply(0, Array(1000))
        .map((x, i) => i)
        .map(i => coll.save({ test: i }))
    );
    let cc = await coll.count();
    console.log(cc);
    expect(cc.count).to.be.eq(1000);

    // wait for followers to get in sync
    const inSync = await instanceManager.asyncReplicationTicksInSync(60.0);
    expect(inSync).to.equal(
      true,
      "followers did not get in sync before timeout"
    );

    console.log("killing leader %s", leader.endpoint);
    await instanceManager.kill(leader);

    uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
    leader = await instanceManager.asyncReplicationLeaderInstance();

    // just to be sure the foxx queue got a chance to run
    await sleep(3000);

    leaderUrl = endpointToUrl(leader.endpoint);
    console.log("checking service on new leader %s", leader.endpoint);
    await checkAllServices({
      endpointUrl: endpointToUrl(leader.endpoint),
      serviceInfos: serviceInfos
    });

    db = arangojs({ url: leaderUrl, databaseName: "_system" });
    cc = await db.collection("testcollection").count();
    expect(cc.count).to.be.eq(1000);
  });
});

async function installAndCheckServices(services) {
  await crudAndCheckServices(services, async function(db, mount, service) {
    await db.installService(mount, service);
  });
}

async function crudAndCheckServices(services, crud) {
  await crudAllServices(services, crud);
  await checkAllServices(services);
}

async function crudAllServices(services, crud) {
  const servicePerMount = new Map(
    services.serviceInfos.map(info => [info.mount, info.service])
  );
  await crudServices(services.endpointUrl, servicePerMount, crud);
}

async function checkAllServices(services) {
  const resultPerMount = new Map(
    services.serviceInfos.map(info => [info.mount, info.result])
  );
  await checkServices(services.endpointUrl, resultPerMount, checkBundleExists);
  await checkServices(
    services.endpointUrl,
    resultPerMount,
    checkServiceAvailable
  );
  const checksumPerMount = new Map(
    services.serviceInfos.map(info => [info.mount, info.checksum])
  );
  await checkServices(
    services.endpointUrl,
    checksumPerMount,
    checkBundleChecksumInCollection
  );
  await checkServices(
    services.endpointUrl,
    checksumPerMount,
    checkBundleChecksum
  );
  const configPerMount = new Map(
    services.serviceInfos.map(info => [info.mount, info.config])
  );
  await checkServices(services.endpointUrl, configPerMount, checkServiceConfig);
  const dependenciesPerMount = new Map(
    services.serviceInfos.map(info => [info.mount, info.dependencies])
  );
  await checkServices(
    services.endpointUrl,
    dependenciesPerMount,
    checkServiceDependencies
  );
}

async function crudServices(endpointUrl, services, crud) {
  const db = arangojs(endpointUrl);
  for (const [mount, service] of services) {
    await crud(db, mount, service);
  }
}

async function configServices(endpointUrl, configs, urConfig) {
  const db = arangojs(endpointUrl);
  for (const [mount, config] of configs) {
    await urConfig(db, mount, config);
  }
}

async function checkServices(endpointUrl, services, check) {
  for (const [mount, expectedResult] of services) {
    if (expectedResult != undefined)
      await check(endpointUrl, mount, expectedResult);
  }
}

async function checkServiceAvailable(endpointUrl, mount, expectedResult) {
  if (expectedResult !== undefined) {
    let response;
    const db = await arangojs(endpointUrl);
    try {
      response = await db.route(mount).get();
    } catch (e) {
      if (expectedResult === null) {
        expect(e.code).to.equal(404);
        const collection = db.collection("_apps");
        const cursor = await db.query(
          aql`FOR s IN ${collection}
              FILTER s.mount == ${service.mount}
              RETURN s`
        );
        expect(await cursor.hasNext()).to.equal(false);
        return;
      }
      throw e;
    }
    expect(response).to.have.property("body", expectedResult);
  }
}

async function checkBundleExists(endpointUrl, mount, expectedResult) {
  if (expectedResult !== undefined) {
    let response;
    try {
      response = await arangojs(endpointUrl)
        .route(UTIL_MOUNT)
        .head({ mount });
    } catch (e) {
      if (expectedResult === null) {
        expect(e.code).to.equal(404);
        return;
      }
      throw new Error(
        "Exception during bundle check: %s",
        e.errorMessage || e.errorNum
      );
    }
    expect(response.statusCode).to.equal(200);
  }
}

async function checkBundleChecksum(endpointUrl, mount, expectedChecksum) {
  const response = await arangojs(endpointUrl)
    .route(UTIL_MOUNT)
    .get("/checksums", { mount });
  expect(response).to.have.property("body");
  if (expectedChecksum === null) {
    expect(response.body).to.eql({});
  } else {
    expect(response.body).to.eql({ [mount]: expectedChecksum });
  }
}

async function checkBundleChecksumInCollection(
  endpointUrl,
  mount,
  expectedChecksum
) {
  if (expectedChecksum !== null) {
    const db = await arangojs(endpointUrl);
    const collection = db.collection("_apps");
    const cursor = await db.query(
      aql`FOR service IN ${collection}
          FILTER service.mount == ${mount}
          RETURN service.checksum`
    );
    expect(await cursor.next()).to.be.equal(expectedChecksum);
  }
}

async function checkServiceConfig(endpointUrl, mount, expectedConfig) {
  const response = await arangojs(endpointUrl).getServiceConfiguration(mount);
  checkConfig(response, expectedConfig);
}

async function checkServiceDependencies(
  endpointUrl,
  mount,
  expectedDependencies
) {
  const response = await arangojs(endpointUrl).getServiceDependencies(mount);
  checkConfig(response, expectedDependencies);
}

function checkConfig(response, expectedConfig) {
  if (expectedConfig === null) {
    expect(response).to.eq({});
  } else {
    for (const key of Object.keys(expectedConfig)) {
      expect(response[key].current).to.be.equal(expectedConfig[key]);
    }
  }
}

async function installUtilService(endpoint) {
  await crudServices(
    endpointToUrl(endpoint),
    [[UTIL_MOUNT, utilService]],
    async function(db, mount, service) {
      await db.installService(mount, service);
    }
  );
}
