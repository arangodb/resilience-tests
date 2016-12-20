'use strict';
const createEndpoint = require('./common.js').createEndpoint;
const which = require('which');
const crypto = require('crypto');
const portFromEndpoint = require('./common.js').portFromEndpoint;
const startInstance = require('./common.js').startInstance;
const exec = require('child_process').exec;

class DockerRunner {
  constructor (image) {
    var currentDate = (new Date()).valueOf().toString();
    var random = Math.random().toString();
    this.prefix = 'arango-' + crypto.createHash('md5').update(currentDate + random).digest('hex');
    this.image = image;
    this.containerNames = [];
  }

  createEndpoint () {
    return createEndpoint();
  }

  locateDocker () {
    if (!this.docker) {
      this.docker = new Promise((resolve, reject) => {
        which('docker', (err, path) => {
          if (err) {
            reject(err);
          } else {
            resolve(path);
          }
        });
      });
    }
    return this.docker;
  }

  containerName (instance) {
    return this.prefix + '-' + instance.name;
  }

  firstStart (instance) {
    return this.locateDocker()
    .then(dockerBin => {
      instance.binary = dockerBin;
      instance.args.push('--server.endpoint=tcp://0.0.0.0:8529');
      let dockerArgs = [
        'run',
        '-e', 'ARANGO_NO_AUTH=1',
        '-p', portFromEndpoint(instance.endpoint) + ':8529',
        '--name=' + this.containerName(instance),
        this.image,
        'arangod'
      ];

      instance.args = dockerArgs.concat(instance.args);
      return instance;
    })
    .then(instance => {
      this.containerNames.push(this.containerName(instance));
      return startInstance(instance);
    });
  }

  restart (instance) {
    if (!instance.realArgs) {
      instance.realArgs = instance.args;
    }
    instance.args = [
      'start',
      '-a',
      this.containerName(instance)
    ];
    return startInstance(instance);
  }

  destroy (instance) {
    return this.locateDocker()
    .then(dockerBin => {
      return new Promise((resolve, reject) => {
        exec(dockerBin + ' rm -fv ' + this.containerName(instance), err => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    });
  }

  cleanup () {
    return Promise.all(this.containerNames.map(containerName => {
      return this.locateDocker()
      .then(dockerBin => {
        return new Promise((resolve, reject) => {
          exec(dockerBin + ' rm -fv ' + containerName, err => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        });
      });
    }))
    .then(() => {
      this.containerNames = [];
    });
  }
}

module.exports = DockerRunner;
