const path = require('path');
const fs = require('fs');
const ip = require('ip');
const startInstance = require('./common.js').startInstance;
const findFreePort = require('./common.js').findFreePort;
const portFromEndpoint = require('./common.js').portFromEndpoint;
const tmp = require('tmp');
const rmRf = require('rimraf-promise');

class LocalRunner {
  constructor(basePath) {
    this.rootDir = tmp.dirSync().name;
    this.basePath = basePath;
  }

  createEndpoint() {
    let myIp = ip.address();

    return findFreePort(myIp)
    .then(port => {
      return 'tcp://' + myIp + ':' + port;
    });
  }

  firstStart(instance) {
    let dir = path.join(this.rootDir, instance.name);
    let dataDir = path.join(dir, 'data');
    let appsDir = path.join(dir, 'apps');
    
    instance.args.push('--javascript.startup-directory=' + path.join(this.basePath, 'js'));
    instance.args.push('--javascript.app-path=' + appsDir);
    instance.args.push(dataDir);

    instance.binary = path.join(this.basePath, 'build', 'bin', 'arangod');
    return startInstance(instance);
  }

  restart(instance) {
    return startInstance(instance);
  }

  cleanup() {
    return rmRf(this.rootDir);
  }
}

module.exports = LocalRunner;
