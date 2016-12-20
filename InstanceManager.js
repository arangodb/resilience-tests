'use strict';
const _ = require('lodash');
const rp = require('request-promise');
const LocalRunner = require('./LocalRunner.js');
const DockerRunner = require('./DockerRunner.js');
const endpointToUrl = require('./common.js').endpointToUrl;

class InstanceManager {
  constructor (name) {
    this.instances = [];

    if (process.env.RESILIENCE_ARANGO_BASEPATH) {
      this.runner = new LocalRunner(process.env.RESILIENCE_ARANGO_BASEPATH);
    } else if (process.env.RESILIENCE_DOCKER_IMAGE) {
      this.runner = new DockerRunner(process.env.RESILIENCE_DOCKER_IMAGE);
    }

    if (!this.runner) {
      throw new Error('Must specify RESILIENCE_ARANGO_BASEPATH (source root dir including a "build" folder containing compiled binaries or RESILIENCE_DOCKER_IMAGE to test a docker container');
    }
    this.currentLog = '';
  }

  startArango (name, endpoint, role, args) {
    args.push('--server.authentication=false');

    let instance = {
      name,
      role,
      process: null,
      status: 'NEW',
      exitcode: null,
      endpoint,
      args,
      logFn: line => {
        if (line.trim().length > 0) {
          let logLine = instance.name + '(' + process.pid + '): \t' + line;
          if (process.env.LOG_IMMEDIATE) {
            console.log(line);
          } else {
            this.currentLog += logLine + '\n';
          }
        }
      }
    };
    return this.runner.firstStart(instance);
  }

  startDbServer (name) {
    return this.runner.createEndpoint()
    .then(endpoint => {
      let args = [
        '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
        '--cluster.my-role=PRIMARY',
        '--cluster.my-local-info=' + name,
        '--cluster.my-address=' + endpoint
      ];
      return this.startArango(name, endpoint, 'primary', args);
    });
  }

  getAgencyEndpoint () {
    return this.instances.filter(instance => {
      return instance.role === 'agent';
    })[0].endpoint;
  }

  startCoordinator (name) {
    return this.runner.createEndpoint()
    .then(endpoint => {
      let args = [
        '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
        '--cluster.my-role=COORDINATOR',
        '--cluster.my-local-info=' + name,
        '--cluster.my-address=' + endpoint
      ];
      return this.startArango(name, endpoint, 'coordinator', args);
    });
  }

  replace (instance) {
    const i = this.instances.indexOf(instance);
    let promise;
    if (instance.status === 'EXITED') {
      promise = Promise.resolve();
    } else {
      promise = this.kill(instance);
    }
    return promise
    .then(() => {
      let args = [
        '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
        '--cluster.my-role=' + instance.role.toUpperCase(),
        '--cluster.my-local-info=' + instance.name,
        '--cluster.my-address=' + instance.endpoint
      ];
      return this.startArango(instance.name, instance.endpoint, instance.role, args);
    })
    .then((instance) => this.waitForInstance(instance))
    .then((instance) => {
      this.instances = [
        ...this.instances.slice(0, i),
        instance,
        ...this.instances.slice(i + 1)
      ];
      return instance;
    });
  }

  startAgency (options = {}) {
    let size = options.agencySize || 1;
    if (options.agencyWaitForSync === undefined) {
      options.agencyWaitForSync = false;
    }
    const wfs = options.agencyWaitForSync;
    let promise = Promise.resolve([]);
    for (var i = 0; i < size; i++) {
      promise = promise.then(instances => {
        return this.runner.createEndpoint()
        .then(endpoint => {
          let index = instances.length;
          let args = [
            '--agency.activate=true',
            '--agency.size=' + size,
            '--agency.pool-size=' + size,
            '--agency.wait-for-sync=' + wfs,
            '--agency.supervision=true',
            '--agency.my-address=' + endpoint
          ];
          if (index === 0) {
            args.push('--agency.endpoint=' + endpoint);
          } else {
            args.push('--agency.endpoint=' + instances[0].endpoint);
          }
          return this.startArango('agency-' + (index + 1), endpoint, 'agent', args);
        })
        .then(instance => {
          return [...instances, instance];
        });
      });
    }
    return promise
    .then(agents => {
      this.instances = agents;
      return agents;
    });
  }

  startCluster (numAgents, numCoordinators, numDbServers, options = {}) {
    let agencyOptions = options.agents || {};
    _.extend(agencyOptions, {agencySize: numAgents});

    return this.startAgency(agencyOptions)
    .then(agents => {
      let promises = [Promise.resolve(agents)];

      let coordinatorOptions = options.coordinators || {};
      let coordinators = Array.from(Array(numDbServers).keys()).reduce((servers, index) => {
        return servers.then(instances => {
          return this.startCoordinator('coordinator-' + (index + 1), coordinatorOptions)
          .then(instance => {
            return [...instances, instance];
          });
        });
      }, Promise.resolve([]));
      promises.push(coordinators);

      let dbServerOptions = options.dbservers || {};
      let dbServers = Array.from(Array(numDbServers).keys()).reduce((dbServers, index) => {
        return dbServers.then(instances => {
          return this.startDbServer('dbServer-' + (index + 1), dbServerOptions)
          .then(instance => {
            return [...instances, instance];
          });
        });
      }, Promise.resolve([]));
      promises.push(dbServers);
      return Promise.all(promises);
    })
    .then(serverGroups => {
      let servers = [];
      serverGroups.forEach(serverGroup => {
        servers = servers.concat(serverGroup);
      });
      return servers;
    })
    .then(servers => {
      this.instances = servers;
      return this.waitForAllInstances();
    })
    .then(() => {
      return this.getEndpoint();
    });
  }

  waitForInstance (instance) {
    if (instance.status !== 'RUNNING') {
      return Promise.reject(new Error('Instance ' + instance.name + ' is down!'));
    }
    return rp(endpointToUrl(instance.endpoint) + '/_api/version')
    .then(() => {
      return instance;
    }, () => {
      return new Promise((resolve, reject) => {
        setTimeout(resolve, 100);
      })
      .then(() => {
        return this.waitForInstance(instance);
      });
    });
  }

  waitForAllInstances () {
    return Promise.all(this.instances.map(instance => {
      return this.waitForInstance(instance);
    }));
  }

  getEndpoint (instance) {
    return (instance || this.coordinators()[0]).endpoint;
  }

  getEndpointUrl (instance) {
    return endpointToUrl(this.getEndpoint());
  }

  check () {
    return this.instances.every(instance => {
      return instance.status === 'RUNNING';
    });
  }

  shutdownCluster () {
    let shutdownPromise;
    if (this.coordinators().length === 0) {
      shutdownPromise = Promise.all(this.agents().map(agent => {
        return agent.process.kill();
      }));
    } else {
      shutdownPromise = rp({
        method: 'DELETE',
        uri: endpointToUrl(this.getEndpoint()) + '/_admin/shutdown?shutdown_cluster=1'
      });
    }
    return shutdownPromise
    .then(() => {
      let checkDown = () => {
        let allDown = this.instances.every(instance => {
          return instance.status === 'EXITED';
        });

        if (allDown) {
          return Promise.resolve(true);
        } else {
          return new Promise((resolve, reject) => {
            setTimeout(() => {
              checkDown.bind(this)().then(resolve, reject);
            }, 100);
          });
        }
      };
      return checkDown();
    });
  }

  cleanup () {
    return this.shutdownCluster()
    .then(() => {
      this.instances = [];
      return this.runner.cleanup();
    })
    .then(() => {
      let log = this.currentLog;
      this.currentLog = '';
      return log;
    });
  }

  dbServers () {
    return this.instances.filter(instance => {
      return instance.role === 'primary';
    });
  }

  coordinators () {
    return this.instances.filter(instance => {
      return instance.role === 'coordinator';
    });
  }

  agents () {
    return this.instances.filter(instance => {
      return instance.role === 'agent';
    });
  }

  kill (instance, signal = 'SIGTERM') {
    let index = this.instances.indexOf(instance);
    if (index === -1) {
      throw new Error('Couldn\'t find instance ' + instance.name);
    }

    instance.process.kill(signal);
    instance.status = 'KILLED';
    return new Promise((resolve, reject) => {
      let check = function () {
        if (instance.status !== 'EXITED') {
          setTimeout(check, 50);
        } else {
          resolve();
        }
      };
      check();
    });
  }

  destroy (instance) {
    let promise;
    if (this.instances.includes(instance)) {
      promise = this.kill(instance);
    } else {
      promise = Promise.resolve();
    }
    return promise
    .then(() => this.runner.destroy(instance))
    .then(() => {
      const i = this.instances.indexOf(instance);
      if (i !== -1) {
        this.instances = [
          ...this.instances.slice(0, i),
          ...this.instances.slice(i + 1)
        ];
      }
    });
  }

  restart (instance) {
    let index = this.instances.indexOf(instance);
    if (index === -1) {
      throw new Error('Couldn\'t find instance ' + instance.name);
    }

    return this.runner.restart(instance)
    .then(() => {
      return this.waitForInstance(instance);
    });
  }
}

module.exports = InstanceManager;
