const path = require('path');
const startInstance = require('./common.js').startInstance;
const findFreePort = require('./common.js').findFreePort;
const portFromEndpoint = require('./common.js').portFromEndpoint;
const createEndpoint = require('./common.js').createEndpoint;
const tmp = require('tmp');
const rmRf = require('rimraf-promise');

class LocalRunner {
  constructor(basePath) {
    this.rootDir = tmp.dirSync({'prefix': 'arango-resilience'}).name;
    this.basePath = basePath;
  }

  createEndpoint() {
    return createEndpoint();
  }

  firstStart(instance) {
    let dir = path.join(this.rootDir, instance.name);
    let dataDir = path.join(dir, 'data');
    let appsDir = path.join(dir, 'apps');
    
    instance.args.unshift('--configuration=none');
    instance.args.push('--javascript.startup-directory=' + path.join(this.basePath, 'js'));
    instance.args.push('--javascript.app-path=' + appsDir);
    instance.args.push('--server.endpoint=' + instance.endpoint);
    instance.args.push(dataDir);

    instance.binary = path.join(this.basePath, 'build', 'bin', 'arangod');
    return startInstance(instance);
  }

  restart(instance) {
    return startInstance(instance);
  }

  cleanup() {
    return rmRf(this.rootDir)
  }
}

module.exports = LocalRunner;
