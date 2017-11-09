"use strict";
const path = require("path");
const startInstance = require("./common.js").startInstance;
const createEndpoint = require("./common.js").createEndpoint;
const tmp = require("tmp");
const rmRf = require("rimraf-promise");
const mkdirp = require("mkdirp-promise/lib/node5");

class LocalRunner {
  constructor(basePath) {
    this.rootDir = tmp.dirSync({ prefix: "arango-resilience" }).name;
    this.basePath = basePath;
  }

  createEndpoint() {
    return createEndpoint();
  }

  firstStart(instance) {
    const dir = path.join(this.rootDir, instance.name);
    const dataDir = path.join(dir, 'data');
    const appsDir = path.join(dir, 'apps');

    instance.args.unshift('--configuration=none');
    instance.args.push(...[
      `--javascript.startup-directory=${path.join(this.basePath, 'js')}`,
      `--javascript.app-path=${appsDir}`,
      `--server.endpoint=${instance.endpoint}`,
      dataDir
    ]);

    const arangod = path.join(this.basePath, "build", "bin", "arangod");
    if (process.env.RESILIENCE_ARANGO_WRAPPER) {
      let wrapper = process.env.RESILIENCE_ARANGO_WRAPPER.split(" ");
      instance.binary = wrapper.shift();
      instance.args = wrapper.concat(arangod, instance.args);
    } else {
      instance.binary = arangod;
    }

    return Promise.all([mkdirp(dataDir), mkdirp(appsDir)]).then(() => {
      return startInstance(instance);
    });
  }

  updateEndpoint(instance, endpoint) {
    instance.endpoint = endpoint;
  }

  restart(instance) {
    return startInstance(instance);
  }

  destroy(instance) {
    return rmRf(path.join(this.rootDir, instance.name));
  }

  cleanup() {
    return rmRf(this.rootDir);
  }
}

module.exports = LocalRunner;
