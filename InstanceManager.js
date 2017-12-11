'use strict';
const _ = require('lodash');
const arangojs = require('arangojs');
const rp = require('request-promise');
const LocalRunner = require('./LocalRunner.js');
const DockerRunner = require('./DockerRunner.js');
const common = require('./common.js');
const endpointToUrl = common.endpointToUrl;
const ERR_IN_SHUTDOWN = 30;
const WAIT_TIMEOUT = 400; // seconds
const debugLog = (logLine) => {
  if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
    console.log((new Date()).toISOString(), logLine);
  };
};

const shutdownKillAttempts = 3600;  // 180s, after this time we kill the instance
const shutdownMaxAttempts = 4000;   // 200s, note that the cluster internally
                                    // has a 120s timeout
const shutdownWaitInterval = 50;    // We check every 50ms if a server is down

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
    this.singleServerCounter = 0;
  }

  async rpAgency(o) {
    let count = 0;
    let delay = 100;
    o.followAllRedirects = true;
    while (true) {
      try {
        return await rp(o);
      } catch(e) {
        if (e.statusCode !== 503 && count++ < 100) {
          throw e;
        }
      }
      await new Promise.delay(delay);
      delay = delay * 2; if (delay > 8000) { delay = 8000; }
    }
  }

  async rpAgencySingleWrite(o) {
    // This can be used if the body of the request contains a single writing
    // transaction which contains a clientID as third element. If we get a
    // 503 we try /_api/agency/inquire until we get a definitive answer as
    // to whether the call has worked or not.
    if (!Array.isArray(o.body) || o.body.length !== 1 ||
        !Array.isArray(o.body[0]) || o.body.length !== 3 ||
        typeof o.body[0][2] !== 'string') {
      console.log("Illegal use of rpAgencySingleWrite!");
      return this.rpWrite(o);
    }
    let count = 0;
    let delay = 100;
    let oo;
    o.followAllRedirects = true;
    let isInquiry = false;
    while (true) {
      if (!isInquiry) {
        try {
          return await rp(o);
        } catch(e) {
          if (e.statusCode !== 503 && count++ < 100) {
            throw e;
          }
          isInquiry = true;  // switch to inquiry mode
        }
      } else {
        let oo = { method: o.method, url: o.url.replace("write", "inquire"),
                   json: o.json, body: [o.body[0][2]],
                   followAllRedirects: true };
        let res;
        // If this throws, we fail:
        res = await rp(oo);
        if (Array.isArray(res.body) && res.body.length == 1 &&
            Array.isArray(res.body[0])) {
          if (res.body[0].length == 1 &&
              typeof res.body[0][0] == 'number') {
            res.body = res.body[0];
            return res;  // this is a bit of a fake because the URL is now
                         // /_api/agency/inquire, but never mind.
          } else if (res.body[0].length == 0) {
            isInquiry = false;  // try again normally
          } else {
            throw new Error('Illegal answer from /_api/agency/inquire');
          }
        } else {
          throw new Error('Illegal answer from /_api/agency/inquire');
        }
      }
      await new Promise.delay(delay);
      delay = delay * 2; if (delay > 8000) { delay = 8000; }
    }
  }

  startArango(name, endpoint, role, args) {
    args.push('--server.authentication=false');
    //args.push('--log.level=v8=debug')

    if (process.env.LOG_COMMUNICATION && process.env.LOG_COMMUNICATION !== '') {
        args.push(`--log.level=communication=${process.env.LOG_COMMUNICATION}`);
    }

    if (process.env.LOG_REQUESTS && process.env.LOG_REQUESTS !== '') {
        args.push(`--log.level=requests=${process.env.LOG_REQUESTS}`);
    }

    if (process.env.LOG_AGENCY && process.env.LOG_AGENCY !== '') {
        args.push(`--log.level=agency=${process.env.LOG_AGENCY}`);
    }

    args.push(`--server.storage-engine=${this.storageEngine}`);

    if (process.env.ARANGO_EXTRA_ARGS) {
      args.push(...process.env.ARANGO_EXTRA_ARGS.split(' '));
    }

    const instance = {
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
            `${instance.name}(${instance.process.pid}): \t${line}`;

          if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
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

  async startSingleServer(nameIn, num = 1) {
    const newInst = [];
    for (let i = 0; i < num; i++) {
      const name = `${nameIn}-${++this.singleServerCounter}`;
      const ep = await this.runner.createEndpoint();
      const inst = await this.startArango(name, ep,
        'single', [
          `--cluster.agency-endpoint=${this.getAgencyEndpoint()}`,
          `--cluster.my-role=SINGLE`,
          `--cluster.my-address=${ep}`,
          `--replication.automatic-failover=true`
      ]);
      this.instances.push(inst);
      newInst.push(inst);
    }
    return newInst;
  }

  async startDbServer(name) {
    this.dbServerCounter++;
    const endpoint = await this.runner.createEndpoint();
    const args = [
      `--cluster.agency-endpoint=${this.getAgencyEndpoint()}`,
      `--cluster.my-role=PRIMARY`,
      `--cluster.my-address=${endpoint}`
    ];
    const instance = await this.startArango(name, endpoint, 'primary', args);
    this.instances.push(instance);
    return instance;
  }

  getAgencyEndpoint() {
    return this.instances.filter(instance => {
      return instance.role === 'agent';
    })[0].endpoint;
  }

  async startCoordinator(name) {
    this.coordinatorCounter++;
    const endpoint = await this.runner.createEndpoint();
    const args = [
      '--cluster.agency-endpoint=' + this.getAgencyEndpoint(),
      '--cluster.my-role=COORDINATOR',
      '--cluster.my-address=' + endpoint,
      '--log.level=requests=trace'
    ];
    const instance = await this.startArango(name, endpoint, 'coordinator', args);
    this.instances.push(instance);
    return instance;
  }

  async replace(instance) {
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
      '--cluster.my-address=' + instance.endpoint
    ];
    instance = await this.startArango(name, instance.endpoint, instance.role, args);
    await this.waitForInstance(instance);
    this.instances.push(instance);
    return instance;
  }

  async startAgency(options = {}) {
    debugLog("starting agencies");
    let size = options.agencySize || 1;
    if (options.agencyWaitForSync === undefined) {
      options.agencyWaitForSync = false;
    }
    const wfs = options.agencyWaitForSync;
    const instances = [];
    let compactionStep = "200";
    let compactionKeep = "100";
    if (process.env.AGENCY_COMPACTION_STEP) {
      compactionStep = process.env.AGENCY_COMPACTION_STEP;
    }
    if (process.env.AGENCY_COMPACTION_KEEP) {
      compactionKeep = process.env.AGENCY_COMPACTION_KEEP;
    }
    for (var i = 0; i < size; i++) {
      const endpoint = await this.runner.createEndpoint();
      const args = [
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
        instances.length ?
          `--agency.endpoint=${instances[0].endpoint}` : `--agency.endpoint=${endpoint}`
      ];
      this.agentCounter++;
      debugLog("booting agency");
      const instance = await this.startArango(
        'agency-' + this.agentCounter,
        endpoint,
        'agent',
        args
      );
      instances.push(instance);
    }
    return this.instances = instances;
  }

  /// Lookup the async failover leader in agency
  async asyncReplicationLeaderId() {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());

    let body;
    try {
      body = await rp({
        method: 'POST', json: true,
        uri: `${baseUrl}/_api/agency/read`,
        followAllRedirects: true,      
        body: [['/arango/Plan']]
      });
    } catch(e) {
      return null;
    }
    
    const leader = body[0].arango.Plan.AsyncReplication.Leader;
    if (!leader) {
      return null;
    }
    const servers = Object.keys(body[0].arango.Plan.Singles);
    if (-1 === servers.indexOf(leader)) {
      throw new Error(`AsyncReplication: Leader ${leader} not one of single servers`);
    }

    let singles = this.singleServers();
    if (servers.length !== singles.length) {
      throw new Error(`AsyncReplication: Requested ${numServers}, but ${servers.length} ready`);
    }
    return leader;
  }

  /// wait for leader selection
  async asyncReplicationLeaderSelected(ignore = null) {
    let i = 200;
    while (i-- > 0) {
      let val = await this.asyncReplicationLeaderId();
      if (val !== null && ignore !== val) {
        return val;
      }
      await sleep(100);
    }
    throw new Error("Timout waiting for leader selection");
  }

  /// look into the agency and return the master instance
  /// assumes leader is in agency, does not try again
  async asyncReplicationLeaderInstance() {
    const uuid = await this.asyncReplicationLeaderId();
    if (uuid == null) {
      throw "leader is not in agency";
    }
    console.log("Leader in agency %s", uuid)
    let instance = await this.resolveUUID(uuid);
    if (!instance) {
      throw new Error("Could not find leader instance locally");
    }
    return instance;
  }

  async lastWalTick(url) {
    url = endpointToUrl(url);    
    const body = await rp.get({json: true, uri: `${url}/_api/wal/lastTick`});
    return body.tick;
  }

  /// Wait for servers to get in sync
  async getApplierState(url) {
    url = endpointToUrl(url);
    const body = await rp.get({json: true, uri:`${url}/_api/replication/applier-state?global=true`});
    return body.state;
  }

  /// Wait for servers to get in sync with leader
  async asyncReplicationTicksInSync(timoutSecs = 45.0) {
    let leader = await this.asyncReplicationLeaderInstance();
    const leaderTick = await this.lastWalTick(leader.endpoint);
    console.log("Leader Tick %s = %s", leader.endpoint, leaderTick);
    let followers = this.singleServers().filter(inst => inst.status === 'RUNNING' && 
                                                        inst.endpoint != leader.endpoint);      

    let tttt = Math.ceil(timoutSecs * 2);
    for (let i = 0; i < tttt; i++) {
      const result = await Promise.all(followers.map(async flw => this.getApplierState(flw.endpoint)))
      let unfinished = result.filter(state => 
        common.compareTicks(state.lastProcessedContinuousTick, leaderTick) == -1
      );
      if (unfinished.length == 0) {
        return true;
      } 
      await sleep(500); // 0.5s
      if (i % 2 == 0 && i > tttt / 2) {
        console.log("Unfinished state: %s", JSON.stringify(unfinished));
      }
    }
    return false;
  }
  
  async findPrimaryDbServer(collectionName) {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    let [info] = await this.rpAgency({
      method: 'POST',
      uri: baseUrl + '/_api/agency/read',
      json: true,
      body: [
        [
          '/arango/Plan/Collections/_system',
          '/arango/Current/ServersRegistered'
        ]
      ]
    });

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
      if (dbServers.length === 0) {
        throw new Error(`Unknown endpoint "${endpoint}"`);
      }
      return dbServers[0];
    }
    throw new Error(`Unknown collection "${collectionName}"`);
  }

  async startCluster(numAgents, numCoordinators, numDbServers, options = {}) {
    let agencyOptions = options.agents || {};
    _.extend(agencyOptions, {agencySize: numAgents});

    debugLog(`Starting cluster A:${numAgents} C:${numCoordinators} D:${numDbServers}`);
    debugLog("booting agents...");

    let agents = await this.startAgency(agencyOptions);
    debugLog("all agents are booted");

    await sleep(2000);
    let dbServers = Array.from(Array(numDbServers).keys()).map(index => {
      return this.startDbServer('dbServer-' + (index + 1));
    });

    debugLog("booting DBServers...");
    await Promise.all(dbServers);
    debugLog("all DBServers are booted");
    await sleep(2000);

    let coordinators = Array.from(
      Array(numCoordinators).keys()
    ).map(index => {
      return this.startCoordinator('coordinator-' + (index + 1));
    });
    debugLog("booting Coordinators...");
    await Promise.all(coordinators);
    debugLog("all Coordinators are booted");

    debugLog("waiting for /_api/version to succeed an all servers");
    await this.waitForAllInstances();
    debugLog("Cluster is up and running");

    return this.getEndpoint();
  }

  async waitForInstance(instance, started = Date.now()) {
    while (true) {
      if (instance.status !== 'RUNNING') {
        throw new Error(`
          Instance ${instance.name} is down!
          Real status ${instance.status} with exitcode ${instance.exitcode}.
          See logfile here ${instance.logFile}`);
      }
     
      let _test_ = Date.now() - started;
      
      if (_test_ > WAIT_TIMEOUT * 1000) {
        console.error("Hass", _test_, WAIT_TIMEOUT * 1000, started, Date.now());
        throw new Error(
          `Instance ${instance.name} is still not ready after ${WAIT_TIMEOUT} secs`
        );
      }

      let ok = false;
      try {
        await rp.get({
          uri: endpointToUrl(instance.endpoint) + '/_api/version'
        });
        return instance;
      } catch (e) {}
      // Wait 100 ms and try again
      await sleep(100);
    }
  }

  async waitForAllInstances() {
    let allWaiters = this.instances.map(instance => this.waitForInstance(instance));
    let results = [];
    for (let waiter of allWaiters) {
      results.push(await waiter);
    }
    return results;
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

  async shutdownCluster() {
    debugLog(`Shutting down the cluster`);
    const nonAgents = [...this.coordinators(), ...this.dbServers(), ...this.singleServers()];

    let allWaiters = nonAgents.map(n => this.shutdown(n));
    for (let waiter of allWaiters) {
      await waiter;
    }
    allWaiters = this.agents().map(a => this.shutdown(a));
    for (let waiter of allWaiters) {
      await waiter;
    }
  }

  async cleanup() {
    await this.shutdownCluster();
    this.instances = [];
    this.agentCounter = 0;
    this.coordinatorCounter = 0;
    this.dbServerCounter = 0;
    this.singleServerCounter = 0;
    await this.runner.cleanup();
    let log = this.currentLog;
    this.currentLog = '';
    return log;
  }

  dbServers() {
    return this.instances.filter(instance => instance.role === 'primary');
  }

  coordinators() {
    return this.instances.filter(instance => instance.role === 'coordinator');
  }

  agents() {
    return this.instances.filter(instance => instance.role === 'agent');
  }

  singleServers() {
    return this.instances.filter(inst => inst.role === 'single');
  }

  /// use Current/ServersRegistered to find the corresponding
  /// instance metadata
  async resolveUUID(uuid) {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    const [info] = await this.rpAgency({
      method: 'POST',
      uri: baseUrl + '/_api/agency/read',
      json: true,
      body: [['/arango/Current/ServersRegistered']]
    });
    const servers = info.arango.Current.ServersRegistered;
    let url = servers[uuid].endpoint;
    return this.instances.filter(inst => inst.endpoint == url).shift();
  }

  async assignNewEndpoint(instance) {
    let index = this.instances.indexOf(instance);
    if (index === -1) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    let endpoint;
    do {
      endpoint = await this.runner.createEndpoint();
    } while (endpoint == instance.endpoint);

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
  }

  // beware! signals are not supported on windows and it will simply do hard kills all the time
  // use shutdown to gracefully stop an instance!
  async kill(instance) {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    instance.process.kill('SIGKILL');
    instance.status = 'KILLED';
    while (true) {
      if (instance.status === 'EXITED') {
        return;
      }
      await sleep(50);
    }
  }

  // beware! signals are not supported on windows and it will simply do hard kills all the time
  // send a STOP signal to halt an instance
  sigstop(instance) {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    instance.process.kill('SIGSTOP');
    instance.status = 'STOPPED';
  }

  sigcontinue(instance) {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    if (instance.status !== 'STOPPED') {
      throw new Exception("trying to send SIGCONT to a process with status " + instance.status);
    }
    
    instance.process.kill('SIGCONT');
    instance.status = 'RUNNING';
  }

  // Internal function do not call from outsite
  async _checkDown(instance) {
    debugLog(`Test if ${instance.name} is down`);
    let attempts = 0;
    while (true) {

      // We leave here with return or throw
      if (instance.status == 'EXITED') {
        debugLog(`${instance.name} is now gone.`);
        return instance;
      }
   
      if (++attempts === shutdownKillAttempts) {
        debugLog(`Sending SIGKILL to ${instance.name}`);
        instance.process.kill('SIGKILL');
        instance.status = 'KILLED';
      } else if (attempts > shutdownMaxAttempts) {
        // Sorry we give up, could not terminate instance with Shutdown and not with SIGKILL
        debugLog(`Failed to shutdown and kill ${instance.name}. Aborting.`);
        throw new Error(instance.name + ' did not stop gracefully after ' + (waitInterval * attempts) + 'ms');
      }

      // wait a while and try again.
      await sleep(shutdownWaitInterval);
    }
  }

  async shutdown(instance) {
    if (instance.status == 'EXITED') {
      return instance;
    }
    debugLog(`Shutdown ${instance.name}`);

    try {
      await rp.delete({ url: this.getEndpointUrl(instance) + '/_admin/shutdown' })
    } catch (err) {
      let expected = false;
      if (err && (err.statusCode === 503 || err.error)) {
        // Some "errors" are expected.
        let errObj = (err.error instanceof String || typeof err.error === "string") ?
                       JSON.parse(err.error) : err.error;
        if (errObj.code == 'ECONNREFUSED') {
          console.warn('hmmm...server ' + instance.name + ' did not respond (' + JSON.stringify(errObj) + '). Assuming it is dead. Status is: ' + instance.status);
          expected = true;
        } else if (errObj.code == 'ECONNRESET') {
          expected = true;
        } else if (err.statusCode === 503) {
          console.warn('server ' + instance.name + ' answered 503. Assuming it is shutting down. Status is: ' + instance.status);
          expected = true;
        } else if (errObj.errorNum === ERR_IN_SHUTDOWN) {
          expected = true;
        }
        if (!expected) {
          console.error("Wir hassen den shutdown: ", errObj.errorNum, " so total sehr " , err.error, "wir haben den typ", typeof err.error);
        }
      }
      if (!expected) {
        console.error("Unhandled error", err);
        throw err;
      }
    }
    return this._checkDown(instance);
  }

  async destroy(instance) {
    if (this.instances.includes(instance)) {
      await this.shutdown(instance);
    }
    await this.runner.destroy(instance);
    const idx = this.instances.indexOf(instance);
    if (idx !== -1) {
      this.instances.splice(idx, 1);
    }
  }

  async restart(instance) {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }
    debugLog(`Restarting ${instance.name}`); 
    await this.runner.restart(instance);
    return this.waitForInstance(instance);
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
    const [info] = await this.rpAgency({
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
    await sleep(2000);
    await Promise.all(this.dbServers().map(dbs => this.restart(dbs)));
    this.restart(fm);
    await sleep(2000);
    await Promise.all(
      this.coordinators()
        .filter(coord => coord !== fm)
        .map(coord => this.restart(coord))
    );
    await this.waitForAllInstances();
  }
}

module.exports = InstanceManager;
