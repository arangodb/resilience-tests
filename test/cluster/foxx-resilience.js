/* global describe, it, beforeEach, afterEach */
"use strict";
const join = require("path").join;
const readFileSync = require("fs").readFileSync;
const InstanceManager = require("../../InstanceManager");
const arangojs = require("arangojs");
const aql = arangojs.aql;
const expect = require("chai").expect;
const {afterEachCleanup} = require("../../utils");

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
const SERVICE_2_CHECKSUM = "23b806e2";
const SERVICE_1_RESULT = "service1";
const SERVICE_2_RESULT = "service2";
const SERVICE_CONFIG = {
  currency: "test1",
  secretKey: "test2"
};
const SERVICE_DEPENDENCIES = {
  mySessions: "test1",
  myAuth: "test2"
};
const MOUNT_1 = "/resiliencetestservice1";
const MOUNT_2 = "/resiliencetestservice2";
const MOUNT_3 = "/resiliencetestservice3";

describe("Foxx service (resilience)", function() {
  describe(
    "while cluster running",
    suiteRunningClusterDifferentServiceSetups(getRandomEndpointUrl)
  );

  describe(
    "after new coordinator added",
    suiteNewCoordinatorDifferentServiceSetups()
  );

  describe(
    "after coordinator rebooted",
    suiteRebootCoordinatorDifferentServiceSetup(getRandomCoordinator)
  );

  describe(
    "after coordinator replaced",
    suiteReplaceCoordinatorDifferentServiceSetup(getRandomCoordinator)
  );

  describe("after cluster start", suiteClusterStartDifferentServiceSetups());

  describe(
    "after development mode disabled",
    suiteDevModeDifferentServiceSetups()
  );
});

function suiteRunningClusterDifferentServiceSetups(getEndpointUrl) {
  return function() {
    describe(
      "with 1 service involved",
      suiteRunningCluster(getEndpointUrl, {
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToUpgrade: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToUninstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: null,
            result: null
          }
        ],
        serviceConfigToUpdate: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT,
            config: SERVICE_CONFIG
          }
        ],
        serviceDependenciesToUpdate: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT,
            dependencies: SERVICE_DEPENDENCIES
          }
        ]
      })
    );
    describe(
      "with 2 services involved",
      suiteRunningCluster(getEndpointUrl, {
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToUpgrade: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          },
          {
            mount: MOUNT_2,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToUninstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: null,
            result: null
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: null,
            result: null
          }
        ],
        serviceConfigToUpdate: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT,
            config: SERVICE_CONFIG
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT,
            config: SERVICE_CONFIG
          }
        ],
        serviceDependenciesToUpdate: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT,
            dependencies: SERVICE_DEPENDENCIES
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT,
            dependencies: SERVICE_DEPENDENCIES
          }
        ]
      })
    );
  };
}

function suiteRunningCluster(getEndpointUrl, params) {
  return function() {
    const im = InstanceManager.create();
    let endpointUrl;

    beforeEach(async function() {
      await im.startCluster(1, 3, 2);
      endpointUrl = await getEndpointUrl(im);
      await installUtilService(im);
    });
    afterEach(() => afterEachCleanup(this, im));

    it("should be installed on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
    });

    it("should be replaced on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      await replaceAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToUpgrade
      });
    });

    it("should be upgraded on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      await upgradeAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToUpgrade
      });
    });

    it("should be uninstalled on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      await uninstallAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToUninstall
      });
    });

    it("should be reconfigured on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      await updateAndCheckServiceConfigurations(im, {
        endpointUrl,
        serviceInfos: params.serviceConfigToUpdate
      });
    });

    it("should its dependencies be reconfigured on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      await updateAndCheckServiceDependencies(im, {
        endpointUrl,
        serviceInfos: params.serviceDependenciesToUpdate
      });
    });
  };
}

function suiteNewCoordinatorDifferentServiceSetups() {
  return function() {
    describe(
      "with 1 service involved",
      suiteNewCoordinator({
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ]
      })
    );
    describe(
      "with 2 services involved",
      suiteNewCoordinator({
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ]
      })
    );
  };
}

function suiteNewCoordinator(params) {
  return function() {
    const im = InstanceManager.create();

    beforeEach(async function() {
      await im.startCluster(1, 3, 2);
      await installUtilService(im);
    });
    afterEach(() => afterEachCleanup(this, im));

    it("should be installed on every coordinator", async function() {
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      const numbCoord = im.coordinators().length;
      await InstanceManager.waitForInstance(await im.startCoordinator("coordinator-new"));
      expect(im.coordinators().length).to.be.above(numbCoord);
      await checkAllServices(im, params.servicesToInstall);
    });

    it("should be removed and unavailable on every coords", async function() {
      const db = arangojs(await getRandomEndpointUrl(im));
      const collection = db.collection("_apps");
      for (const info of params.servicesToInstall) {
        await db.query(
          aql`INSERT {mount: ${info.mount}, checksum: '69'} IN ${collection}`
        );
      }
      const numbCoord = im.coordinators().length;
      await InstanceManager.waitForInstance(await im.startCoordinator("coordinator-new"));
      expect(im.coordinators().length).to.be.above(numbCoord);
      const servicesToCheck = params.servicesToInstall;
      for (const service of servicesToCheck) {
        service.checksum = null;
        service.result = null;
      }
      await checkAllServices(im, servicesToCheck);
    });
  };
}

function suiteRebootCoordinatorDifferentServiceSetup(getCoordinatorInstance) {
  return function() {
    describe(
      "with 1 service involved",
      suiteRebootCoordinator(getCoordinatorInstance, {
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToUpgrade: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToUninstall: [
          {
            mount: MOUNT_1,
            checksum: null,
            result: null
          }
        ]
      })
    );
    describe(
      "with 2 services involved",
      suiteRebootCoordinator(getCoordinatorInstance, {
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToUpgrade: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          },
          {
            mount: MOUNT_2,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToUninstall: [
          {
            mount: MOUNT_1,
            checksum: null,
            result: null
          },
          {
            mount: MOUNT_2,
            checksum: null,
            result: null
          }
        ]
      })
    );
  };
}

function suiteRebootCoordinator(getCoordinatorInstance, params) {
  return function() {
    const im = InstanceManager.create();
    let coordinatorInstance;

    beforeEach(async function() {
      await im.startCluster(1, 3, 2);
      coordinatorInstance = await getCoordinatorInstance(im);
      await installUtilService(im);
    });
    afterEach(() => afterEachCleanup(this, im));

    it("should be installed on every coordinator", async function() {
      await im.shutdown(coordinatorInstance);
      expect(isRunning(coordinatorInstance)).to.be.false;
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      await im.restart(coordinatorInstance);
      expect(isRunning(coordinatorInstance)).to.be.true;
      await checkAllServices(im, params.servicesToInstall);
    });

    it("should be corrected if not equal than in cluster", async function() {
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      await im.shutdown(coordinatorInstance);
      expect(isRunning(coordinatorInstance)).to.be.false;
      await replaceAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToUpgrade
      });
      await im.restart(coordinatorInstance);
      expect(isRunning(coordinatorInstance)).to.be.true;
      await checkAllServices(im, params.servicesToUpgrade);
    });

    it("should be ignored if not installed in cluster", async function() {
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      await im.shutdown(coordinatorInstance);
      expect(isRunning(coordinatorInstance)).to.be.false;
      await uninstallAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToUninstall
      });
      await im.restart(coordinatorInstance);
      expect(isRunning(coordinatorInstance)).to.be.true;
      await checkAllServices(im, params.servicesToUninstall);
    });
  };
}

function suiteReplaceCoordinatorDifferentServiceSetup(getCoordinatorInstance) {
  return function() {
    describe(
      "with 1 service involved",
      suiteReplaceCoordinator(getCoordinatorInstance, {
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToHeal: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: null,
            result: null
          }
        ]
      })
    );
    describe(
      "with 2 services involved",
      suiteReplaceCoordinator(getCoordinatorInstance, {
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToHeal: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: null,
            result: null
          },
          {
            mount: MOUNT_2,
            service: service1,
            checksum: null,
            result: null
          }
        ]
      })
    );
  };
}

function suiteReplaceCoordinator(getCoordinatorInstance, params) {
  return function() {
    const im = InstanceManager.create();
    let coordinatorInstance;

    beforeEach(async function() {
      await im.startCluster(1, 3, 2);
      coordinatorInstance = await getCoordinatorInstance(im);
      await installUtilService(im);
    });
    afterEach(() => afterEachCleanup(this, im));

    it("should be installed on every coordinator", async function() {
      const numbCoord = im.coordinators().length;
      await im.destroy(coordinatorInstance);
      expect(im.coordinators().length).to.be.below(numbCoord);
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      await im.replace(coordinatorInstance);
      expect(im.coordinators().length).to.be.equal(numbCoord);
      await checkAllServices(im, params.servicesToInstall);
    });

    it("should be corrected if not equal than in cluster", async function() {
      for (const service of params.servicesToHeal) {
        await prepopulateServiceFiles(
          await im.getEndpointUrl(coordinatorInstance),
          service.mount,
          service.service
        );
      }
      const numbCoord = im.coordinators().length;
      await im.destroy(coordinatorInstance);
      expect(im.coordinators().length).to.be.below(numbCoord);
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      await im.replace(coordinatorInstance);
      expect(im.coordinators().length).to.be.equal(numbCoord);
      await checkAllServices(im, params.servicesToInstall);
    });

    it("should be ignored if not installed in cluster", async function() {
      for (const service of params.servicesToHeal) {
        await prepopulateServiceFiles(
          await im.getEndpointUrl(coordinatorInstance),
          service.mount,
          service.service
        );
      }
      const numbCoord = im.coordinators().length;
      await im.destroy(coordinatorInstance);
      expect(im.coordinators().length).to.be.below(numbCoord);
      await im.replace(coordinatorInstance);
      expect(im.coordinators().length).to.be.equal(numbCoord);
      await checkAllServices(im, params.servicesToHeal);
    });
  };
}

function suiteClusterStartDifferentServiceSetups() {
  return function() {
    describe(
      "with 1 service involved",
      suiteClusterStart({
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToManipulate: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ]
      })
    );

    describe(
      "with 2 services involved",
      suiteClusterStart({
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToManipulate: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          },
          {
            mount: MOUNT_2,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ]
      })
    );
  };
}

function suiteClusterStart(params) {
  return function() {
    const im = InstanceManager.create();

    beforeEach(async function() {
      await im.startCluster(1, 3, 2);
      await installUtilService(im);
    });
    afterEach(() => afterEachCleanup(this, im));
    it("when missing on any one coordinator should be available on every coordinator", async function() {
      const endpointUrl = await getRandomEndpointUrl(im);
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      for (const service of params.servicesToInstall) {
        await deleteLocalServiceFiles(endpointUrl, service.mount);
      }
      await im.restartCluster();
      await checkAllServices(im, params.servicesToInstall);
    });

    it("when missing on all coordinators should not be available", async function() {
      await installAndCheckServices(im, {
        endpointUrl: await getRandomEndpointUrl(im),
        serviceInfos: params.servicesToInstall
      });
      for (const service of params.servicesToInstall) {
        for (const endpointUrl of getAllCoordEndpointUrls(im)) {
          await deleteLocalServiceFiles(endpointUrl, service.mount);
        }
      }
      await im.restartCluster();
      const servicesToCheck = params.servicesToInstall;
      for (const service of servicesToCheck) {
        service.checksum = null;
        service.result = null;
      }
      await checkAllServices(im, servicesToCheck);
    });

    it("when missing in storage should be available on every coordinator", async function() {
      const endpointUrl = await getRandomEndpointUrl(im);
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      const db = await arangojs(endpointUrl);
      const collection = db.collection("_apps");
      for (const service of params.servicesToInstall) {
        await db.query(
          aql`FOR service IN ${collection}
          FILTER service.mount == ${service.mount}
          REMOVE service in ${collection}`
        );
      }
      await im.restartCluster();
      const servicesToCheck = params.servicesToInstall;
      for (const service of servicesToCheck) {
        service.checksum = null;
        service.result = null;
      }
      await checkAllServices(im, servicesToCheck);
    });

    it("with wrong service on any one coordinator should be available on every coordinator", async function() {
      const endpointUrl = await getRandomEndpointUrl(im);
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      for (const service of params.servicesToManipulate) {
        await deleteLocalServiceFiles(endpointUrl, service.mount);
        await prepopulateServiceFiles(
          endpointUrl,
          service.mount,
          service.service
        );
      }
      await im.restartCluster();
      await checkAllServices(im, params.servicesToInstall);
    });

    it("with missing checksum in storage should be available on every coordinator", async function() {
      const endpointUrl = await getRandomEndpointUrl(im);
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      const db = arangojs(endpointUrl);
      const collection = db.collection("_apps");
      await db.query(
        aql`
          FOR info IN ${params.servicesToInstall}
            FOR service in ${collection}
              FILTER service.mount == info.mount
              UPDATE service
              WITH {checksum: null}
              IN ${collection} OPTIONS { keepNull: false }
        `
      );
      await im.restartCluster();
      await checkAllServices(im, params.servicesToInstall);
    });
  };
}

function suiteDevModeDifferentServiceSetups() {
  return function() {
    describe(
      "with 1 service involved",
      suiteDevMode({
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          }
        ],
        servicesToReplace: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: "d8bce44b",
            result: SERVICE_2_RESULT
          }
        ]
      })
    );
    describe(
      "with 2 services involved",
      suiteDevMode({
        servicesToInstall: [
          {
            mount: MOUNT_1,
            service: service1,
            checksum: SERVICE_1_CHECKSUM,
            result: SERVICE_1_RESULT
          },
          {
            mount: MOUNT_2,
            service: service2,
            checksum: SERVICE_2_CHECKSUM,
            result: SERVICE_2_RESULT
          }
        ],
        servicesToReplace: [
          {
            mount: MOUNT_1,
            service: service2,
            checksum: "d8bce44b",
            result: SERVICE_2_RESULT
          },
          {
            mount: MOUNT_2,
            service: service1,
            checksum: "6d7faf6",
            result: SERVICE_1_RESULT
          }
        ]
      })
    );
  };
}

function suiteDevMode(params) {
  return function() {
    const im = InstanceManager.create();

    beforeEach(async function() {
      await im.startCluster(1, 3, 2);
      await installUtilService(im);
    });
    afterEach(() => afterEachCleanup(this, im));

    it("should be replaced on every coordinator", async function() {
      const endpointUrl = await getRandomEndpointUrl(im);
      await installAndCheckServices(im, {
        endpointUrl,
        serviceInfos: params.servicesToInstall
      });
      const db = await arangojs(endpointUrl);
      for (const service of params.servicesToReplace) {
        await db.enableServiceDevelopmentMode(service.mount);
        await replaceServiceFiles(endpointUrl, service.mount, service.service);
      }
      const resultPerMount = new Map(
        params.servicesToReplace.map(info => [info.mount, info.result])
      );
      await checkServices([endpointUrl], resultPerMount, checkBundleExists);
      await checkServices([endpointUrl], resultPerMount, checkServiceAvailable);
      for (const service of params.servicesToReplace) {
        await db.disableServiceDevelopmentMode(service.mount);
      }
      const checksumPerMount = new Map(
        params.servicesToReplace.map(info => [info.mount, info.checksum])
      );
      await checkServices([endpointUrl], checksumPerMount, checkBundleChecksum);
      await checkServices(
        [endpointUrl],
        checksumPerMount,
        checkBundleChecksumInCollection
      );
      await checkAllServices(im, params.servicesToReplace);
    });
  };
}

function getAllCoordEndpointUrls(im) {
  return im
    .coordinators()
    .filter(instance => isRunning(instance))
    .map(instance => im.getEndpointUrl(instance));
}

function isRunning(instance) {
  return instance.status === "RUNNING";
}

async function getRandomCoordinator(im) {
  return im.coordinators().find(c => isRunning(c));
}

async function getRandomEndpointUrl(im) {
  return im.getEndpointUrl(await getRandomCoordinator(im));
}

async function installAndCheckServices(im, services) {
  await crudAndCheckServices(im, services, async function(db, mount, service) {
    await db.installService(mount, service);
  });
}

async function replaceAndCheckServices(im, services) {
  await crudAndCheckServices(im, services, async function(db, mount, service) {
    await db.replaceService(mount, service);
  });
}

async function upgradeAndCheckServices(im, services) {
  await crudAndCheckServices(im, services, async function(db, mount, service) {
    await db.upgradeService(mount, service);
  });
}

async function uninstallAndCheckServices(im, services) {
  await crudAndCheckServices(im, services, async function(db, mount, service) {
    await db.uninstallService(mount);
  });
}

async function updateAndCheckServiceConfigurations(im, services) {
  await configAndCheckServices(im, services, configAllServices, async function(
    db,
    mount,
    config
  ) {
    await db.updateServiceConfiguration(mount, JSON.stringify(config));
  });
}
async function updateAndCheckServiceDependencies(im, services) {
  await configAndCheckServices(
    im,
    services,
    configDependenciesAllServices,
    async function(db, mount, config) {
      await db.updateServiceDependencies(mount, JSON.stringify(config));
    }
  );
}

async function crudAndCheckServices(im, services, crud) {
  const crudEndpointUrl = services.endpointUrl;
  const serviceInfos = services.serviceInfos;
  await crudAllServices(crudEndpointUrl, serviceInfos, crud);
  await checkAllServices(im, serviceInfos);
}

async function crudAllServices(crudEndpointUrl, serviceInfos, crud) {
  const servicePerMount = new Map(
    serviceInfos.map(info => [info.mount, info.service])
  );
  await crudServices(crudEndpointUrl, servicePerMount, crud);
}

async function configAndCheckServices(im, services, configServices, urConfig) {
  const crudEndpointUrl = services.endpointUrl;
  const serviceInfos = services.serviceInfos;
  await configServices(crudEndpointUrl, serviceInfos, urConfig);
  await checkAllServices(im, serviceInfos);
}

async function configAllServices(crudEndpointUrl, serviceInfos, urConfig) {
  const servicePerMount = new Map(
    serviceInfos.map(info => [info.mount, info.config])
  );
  await configServices(crudEndpointUrl, servicePerMount, urConfig);
}

async function configDependenciesAllServices(
  crudEndpointUrl,
  serviceInfos,
  urConfig
) {
  const servicePerMount = new Map(
    serviceInfos.map(info => [info.mount, info.dependencies])
  );
  await configServices(crudEndpointUrl, servicePerMount, urConfig);
}

async function checkAllServices(im, serviceInfos) {
  const endpointUrls = getAllCoordEndpointUrls(im);
  const resultPerMount = new Map(
    serviceInfos.map(info => [info.mount, info.result])
  );
  await checkServices(endpointUrls, resultPerMount, checkBundleExists);
  await checkServices(endpointUrls, resultPerMount, checkServiceAvailable);
  const checksumPerMount = new Map(
    serviceInfos.map(info => [info.mount, info.checksum])
  );
  await checkServices(
    endpointUrls,
    checksumPerMount,
    checkBundleChecksumInCollection
  );
  await checkServices(endpointUrls, checksumPerMount, checkBundleChecksum);
  const configPerMount = new Map(
    serviceInfos.map(info => [info.mount, info.config])
  );
  await checkServices(endpointUrls, configPerMount, checkServiceConfig);
  const dependenciesPerMount = new Map(
    serviceInfos.map(info => [info.mount, info.dependencies])
  );
  await checkServices(
    endpointUrls,
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

async function checkServices(endpointUrls, services, check) {
  for (const endpointUrl of endpointUrls) {
    for (const [mount, expectedResult] of services) {
      if (expectedResult != undefined)
        await check(endpointUrl, mount, expectedResult);
    }
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
      throw e;
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

async function installUtilService(im) {
  await crudServices(
    await getRandomEndpointUrl(im),
    [[UTIL_MOUNT, utilService]],
    async function(db, mount, service) {
      await db.installService(mount, service);
    }
  );
}

async function deleteLocalServiceFiles(endpointUrl, mount) {
  await arangojs(endpointUrl)
    .route(UTIL_MOUNT)
    .request({ method: "DELETE", qs: { mount } });
}

async function prepopulateServiceFiles(endpointUrl, mount, service) {
  await arangojs(endpointUrl)
    .route(UTIL_MOUNT)
    .request({
      method: "POST",
      body: service,
      isBinary: true,
      qs: { mount }
    });
}

async function replaceServiceFiles(endpointUrl, mount, service) {
  await arangojs(endpointUrl)
    .route(UTIL_MOUNT)
    .request({
      method: "PUT",
      body: service,
      isBinary: true,
      qs: { mount }
    });
}
