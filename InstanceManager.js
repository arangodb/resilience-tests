'use strict';
const _ = require('lodash');
const rp = require('request-promise');
const LocalRunner = require('./LocalRunner.js');
const DockerRunner = require('./DockerRunner.js');
const endpointToUrl = require('./common.js').endpointToUrl;
const WAIT_TIMEOUT = 300; // seconds

function sleep(ms) {
  return new Promise(resolve => setTimeout(() => resolve(), ms));
}

class InstanceManager {
  constructor(name) {
    this.instances = [];

    if (process.env.RESILIENCE_ARANGO_BASEPATH) {
      this.runner = new LocalRunner(process.env.RESILIENCE_ARANGO_BASEPATH);
    } else if (process.env.RESILIENCE_DOCKER_IMAGE) {
      this.runner = new DockerRunner(process.env.RESILIENCE_DOCKER_IMAGE);
    }

    if (process.env.ARANGO_STORAGE_ENGINE) {
      this.storageEngine = process.env.ARANGO_STORAGE_ENGINE;
      if (this.storageEngine !== 'rocksdb') {
        this.storageEngine = 'mmfiles';
      }
    } else {
      this.storageEngine = 'mmfiles';
    }

    if (!this.runner) {
      throw new Error(
        'Must specify RESILIENCE_ARANGO_BASEPATH (source root dir including a "build" folder containing compiled binaries or RESILIENCE_DOCKER_IMAGE to test a docker container'
      );
    }
    this.currentLog = '';
    this.agentCounter = 0;
    this.coordinatorCounter = 0;
    this.dbServerCounter = 0;
  }

  startArango(name, endpoint, role, args) {
    args.push('--server.authentication=false');
    //args.push('--log.level=v8=debug')
    //args.push('--log.level=communication=debug');
    args.push('--server.storage-engine=' + this.storageEngine);

    if (process.env.ARANGO_EXTRA_ARGS) {
      args = args.concat(process.env.ARANGO_EXTRA_ARGS.split(' '));
    }

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
          let logLine =
            instance.name + '(' + instance.process.pid + '): \t' + line;
          if (process.env.LOG_IMMEDIATE) {
            console.log(logLine);
          } else {
            logLine = logLine.replace(/\x1B/g, '');
            this.currentLog += logLine + '\n';
          }
        }
      }
    };
    return this.runner.firstStart(instance);
  }

  startDbServer(name) {
    this.dbServerCounter++;
    return this.runner
      .createEndpoint()
      .then(endpoint => {
        let args = [
          '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
          '--cluster.my-role=PRIMARY',
          '--cluster.my-local-info=' + name,
          '--cluster.my-address=' + endpoint
        ];
        return this.startArango(name, endpoint, 'primary', args);
      })
      .then(instance => {
        this.instances.push(instance);
        return instance;
      });
  }

  getAgencyEndpoint() {
    return this.instances.filter(instance => {
      return instance.role === 'agent';
    })[0].endpoint;
  }

  startCoordinator(name) {
    this.coordinatorCounter++;
    return this.runner
      .createEndpoint()
      .then(endpoint => {
        let args = [
          '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
          '--cluster.my-role=COORDINATOR',
          '--cluster.my-local-info=' + name,
          '--cluster.my-address=' + endpoint
        ];
        return this.startArango(name, endpoint, 'coordinator', args);
      })
      .then(instance => {
        this.instances.push(instance);
        return instance;
      });
  }

  replace(instance) {
    let name, role;
    switch (instance.role) {
      case 'coordinator':
        this.coordinatorCounter++;
        name = 'coordinator-' + this.coordinatorCounter;
        role = 'COORDINATOR';
        break;
      case 'primary':
        this.dbServerCounter++;
        name = 'dbServer-' + this.dbServerCounter;
        role = 'PRIMARY';
        break;
      default:
        throw new Error('Can only replace coordinators/dbServers');
    }
    if (this.instances.includes(instance)) {
      throw new Error('Instance must be destroyed before it can be replaced');
    }
    let args = [
      '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
      '--cluster.my-role=' + role,
      '--cluster.my-local-info=' + name,
      '--cluster.my-address=' + instance.endpoint
    ];
    return this.startArango(name, instance.endpoint, instance.role, args)
      .then(instance => this.waitForInstance(instance))
      .then(instance => {
        this.instances.push(instance);
        return instance;
      });
  }

  startAgency(options = {}) {
    let size = options.agencySize || 1;
    if (options.agencyWaitForSync === undefined) {
      options.agencyWaitForSync = false;
    }
    const wfs = options.agencyWaitForSync;
    let promise = Promise.resolve([]);
    let compactionStep = "200";
    let compactionKeep = "100";
    if (process.env.AGENCY_COMPACTION_STEP) {
      compactionStep = process.env.AGENCY_COMPACTION_STEP;
    }
    if (process.env.AGENCY_COMPACTION_KEEP) {
      compactionKeep = process.env.AGENCY_COMPACTION_KEEP;
    }
    for (var i = 0; i < size; i++) {
      promise = promise.then(instances => {
        return this.runner
          .createEndpoint()
          .then(endpoint => {
            let args = [
              '--agency.activate=true',
              '--agency.size=' + size,
              '--agency.pool-size=' + size,
              '--agency.wait-for-sync=true',
              '--agency.supervision=true',
              '--server.threads=16',
              '--agency.supervision-frequency=0.5',
              '--agency.supervision-grace-period=2.5',
              '--agency.compaction-step-size='+compactionStep,
              '--agency.compaction-keep-size='+compactionKeep,
              '--agency.my-address=' + endpoint,
              '--log.force-direct=true'
            ];
            if (instances.length === 0) {
              args.push('--agency.endpoint=' + endpoint);
            } else {
              args.push('--agency.endpoint=' + instances[0].endpoint);
            }
            this.agentCounter++;
            return this.startArango(
              'agency-' + this.agentCounter,
              endpoint,
              'agent',
              args
            );
          })
          .then(instance => {
            return [...instances, instance];
          });
      });
    }
    return promise.then(agents => {
      this.instances = agents;
      return agents;
    });
  }

  findPrimaryDbServer(collectionName) {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    return rp({
      method: 'POST',
      uri: baseUrl + '/_api/agency/read',
      json: true,
      body: [
        [
          '/arango/Plan/Collections/_system',
          '/arango/Current/ServersRegistered'
        ]
      ]
    }).then(([info]) => {
      const collections = info.arango.Plan.Collections._system;
      const servers = info.arango.Current.ServersRegistered;
      for (const id of Object.keys(collections)) {
        const collection = collections[id];
        if (collection.name !== collectionName) {
          continue;
        }
        const shards = collection.shards;
        const shardsId = Object.keys(shards)[0];
        const uuid = shards[shardsId][0];
        const endpoint = servers[uuid].endpoint;
        const dbServers = this.dbServers().filter(
          instance => instance.endpoint === endpoint
        );
        if (!dbServers.length) {
          return Promise.reject(new Error(`Unknown endpoint "${endpoint}"`));
        }
        return dbServers[0];
      }
      return Promise.reject(
        new Error(`Unknown collection "${collectionName}"`)
      );
    });
  }

  startCluster(numAgents, numCoordinators, numDbServers, options = {}) {
    let agencyOptions = options.agents || {};
    _.extend(agencyOptions, {agencySize: numAgents});

    return this.startAgency(agencyOptions)
      .then(agents => {
        let promises = [Promise.resolve(agents)];

        let coordinators = Array.from(
          Array(numCoordinators).keys()
        ).map(index => {
          return this.startCoordinator('coordinator-' + (index + 1));
        });
        let dbServers = Array.from(Array(numDbServers).keys()).map(index => {
          return this.startDbServer('dbServer-' + (index + 1));
        });
        return Promise.all([].concat(coordinators, dbServers));
      })
      .then(servers => {
        return this.waitForAllInstances();
      })
      .then(() => {
        return this.getEndpoint();
      });
  }

  async waitForInstance(instance, started = Date.now()) {
    if (instance.status !== 'RUNNING') {
      throw new Error(`Instance ${instance.name} is down!`);
    }
    if (Date.now() - started > WAIT_TIMEOUT * 1000) {
      throw new Error(
        `Instance ${instance.name} is still not ready after ${WAIT_TIMEOUT} secs`
      );
    }

    let ok = false;
    try {
      await rp({
        method: 'GET',
        uri: endpointToUrl(instance.endpoint) + '/_api/version'
      });
      ok = true;
    } catch (e) {}
    if (ok) {
      return instance;
    } else {
      await sleep(100);
      return this.waitForInstance(instance, started);
    }
  }

  waitForAllInstances() {
    return Promise.all(
      this.instances.map(instance => {
        return this.waitForInstance(instance);
      })
    );
  }

  getEndpoint(instance) {
    return (instance || this.coordinators()[0]).endpoint;
  }

  getEndpointUrl(instance) {
    return endpointToUrl(this.getEndpoint(instance));
  }

  check() {
    return this.instances.every(instance => {
      return instance.status === 'RUNNING';
    });
  }

  shutdownCluster() {
    let shutdownPromise;

    let nonAgents = [].concat(this.coordinators(), this.dbServers());

    return Promise.all(
      nonAgents.map(server => {
        server.process.kill();
      })
    )
      .then(() => {
        let checkDown = () => {
          let allDown = nonAgents.every(instance => {
            return instance.status === 'EXITED';
          });

          if (allDown) {
            return Promise.resolve(true);
          } else {
            return new Promise((resolve, reject) => {
              setTimeout(() => {
                checkDown.bind(this)().then(resolve, reject);
              }, 5000);
            });
          }
        };
        return checkDown();
      })
      .then(() => {
        return Promise.all(
          this.agents().map(agent => {
            return agent.process.kill();
          })
        );
      })
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

  cleanup() {
    return this.shutdownCluster()
      .then(() => {
        this.instances = [];
        this.agentCounter = 0;
        this.coordinatorCounter = 0;
        this.dbServerCounter = 0;
        return this.runner.cleanup();
      })
      .then(() => {
        let log = this.currentLog;
        this.currentLog = '';
        return log;
      });
  }

  dbServers() {
    return this.instances.filter(instance => {
      return instance.role === 'primary';
    });
  }

  coordinators() {
    return this.instances.filter(instance => {
      return instance.role === 'coordinator';
    });
  }

  agents() {
    return this.instances.filter(instance => {
      return instance.role === 'agent';
    });
  }

  assignNewEndpoint(instance) {
    let index = this.instances.indexOf(instance);
    if (index === -1) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    var createNewEndpoint = () => {
      return this.runner.createEndpoint().then(endpoint => {
        if (endpoint != instance.endpoint) {
          return endpoint;
        } else {
          return createNewEndpoint();
        }
      });
    };

    return createNewEndpoint().then(endpoint => {
      [
        'server.endpoint',
        'agency.my-address',
        'cluster.my-address'
      ].filter(arg => {
        index = instance.args.indexOf('--' + arg + '=' + instance.endpoint);
        if (index !== -1) {
          instance.args[index] = '--' + arg + '=' + endpoint;
        }
      });
      return this.runner.updateEndpoint(instance, endpoint);
    });
  }

  kill(instance, signal = 'SIGTERM') {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    instance.process.kill(signal);
    instance.status = 'KILLED';
    return new Promise((resolve, reject) => {
      let check = function() {
        if (instance.status !== 'EXITED') {
          setTimeout(check, 50);
        } else {
          resolve();
        }
      };
      check();
    });
  }

  destroy(instance) {
    let promise;
    if (this.instances.includes(instance)) {
      promise = this.kill(instance);
    } else {
      promise = Promise.resolve();
    }
    return promise.then(() => this.runner.destroy(instance)).then(() => {
      const i = this.instances.indexOf(instance);
      if (i !== -1) {
        this.instances = [
          ...this.instances.slice(0, i),
          ...this.instances.slice(i + 1)
        ];
      }
    });
  }

  restart(instance) {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    return this.runner.restart(instance).then(() => {
      return this.waitForInstance(instance);
    });
  }

  // this will append the logs to the test in case of a failure so
  // you get a nice combined log of what happened on the server and client
  moveServerLogs(test) {
    if (test.state === 'failed') {
      test.err.message = this.currentLog + '\n\n' + test.err.message;
    }
    this.currentLog = '';
  }

  async getFoxxmaster() {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    const [info] = await rp({
      method: 'POST',
      uri: baseUrl + '/_api/agency/read',
      json: true,
      body: [
        ['/arango/Current/Foxxmaster', '/arango/Current/ServersRegistered']
      ]
    });
    const uuid = info.arango.Current.Foxxmaster;
    const endpoint = info.arango.Current.ServersRegistered[uuid].endpoint;
    return this.instances.find(instance => instance.endpoint === endpoint);
  }

  async restartCluster() {
    const fm = await this.getFoxxmaster();
    await this.shutdownCluster();
    await Promise.all(this.agents().map(agent => this.restart(agent)));
    await Promise.all(this.dbServers().map(dbs => this.restart(dbs)));
    this.restart(fm);
    await sleep(100);
    await Promise.all(
      this.coordinators()
        .filter(coord => coord !== fm)
        .map(coord => this.restart(coord))
    );
    await this.waitForAllInstances();
  }
}

module.exports = InstanceManager;
