/* global describe, it, afterEach */
'use strict';

const InstanceManager = require('../../InstanceManager.js');
const endpointToUrl = require('../../common.js').endpointToUrl;

const rp = require('request-promise');
const arangojs = require('arangojs');
const expect = require('chai').expect;
const sleep = (ms= 1000) => new Promise(resolve => setTimeout(resolve, ms));

/// return the list of endpoints, in a normal cluster this is the list of
/// coordinator endpoints.
async function requestEndpoints(url) {
  url = endpointToUrl(url);
  const body = await rp.get({ uri: `${url}/_api/cluster/endpoints`, json: true});
  if (body.error ) {
    throw new Error(body);
  }
  if (!body.endpoints || body.endpoints.length == 0) {
    throw new Error(`AsyncReplication: not all servers ready. Have ${body.endpoints.length} servers`);
  }
  return body.endpoints;
};

/*
  async asyncReplicationMasterCon(db = '_system') {
    const leader = await this.asyncReplicationLeaderInstance();    
    return arangojs({ url: leader.endpoint, databaseName: db });
  }

  asyncReplicationCons(db = '_system') {
    return this.singleServers().map(inst =>
      arangojs({ url: endpointToUrl(inst.endpoint), databaseName: db }));
  }
*/

describe('Synchronize tick values', async function() {
  const instanceManager = new InstanceManager('setup');

  beforeEach(async function(){
    await instanceManager.startAgency({agencySize:1});        
  });

  afterEach(function() {
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup();
  });

  /// check tick values synchronize, check endpoints
  /// TODO check for redirects to leader
  async function doAsyncReplChecks(n, leader) {
    console.log("Leader selected, waiting for tick synchronization...");
    const inSync = await instanceManager.asyncReplicationTicksInSync();
    expect(inSync).to.equal(true, "slaves did not get in sync before timeout");

    // wait at least 0.5s + 2.5s for agency supervision
    // to persist the health status
    //await sleep(5000);    

    console.log("Checking endpoints...");
    /// make sure all servers know the leader
    let servers = instanceManager.singleServers();
    //expect(servers).to.have.lengthOf(n);
    for (let x = 0; x < servers.length; x++) {
      let url = endpointToUrl(servers[x].endpoint);
      let body = await rp.get({ uri: `${url}/_admin/server/role`, json: true});
      expect(body.mode).to.equal("resilient", `Wrong response ${JSON.stringify(body)}`);
      //  TODO check location header on other APIs

      let list = await requestEndpoints(servers[x].endpoint);
      // TODO only works after waiting ~5s for agency supervision
      //expect(list).to.have.lengthOf(n, "Endpoints: " + JSON.stringify(list));
      expect(leader.endpoint).to.equal(list[0]);
    }
  }

  for (let n = 2; n <= 8; n *= 2) {
    it(`for ${n} servers`, async function() {
      await instanceManager.startSingleServer('single', n);
      await instanceManager.waitForAllInstances();

      // get current leader
      await instanceManager.asyncReplicationLeaderSelected();
      const leader = await instanceManager.asyncReplicationLeaderInstance();

      await doAsyncReplChecks(n, leader);
    });
  }

  /*let n = 2;
  it(`for ${n} servers with failover`, async function() {
    await instanceManager.startSingleServer('single', n);
    await instanceManager.waitForAllInstances();

    // wait for leader selection
    let uuid = await instanceManager.asyncReplicationLeaderSelected();
    let leader = await instanceManager.asyncReplicationLeaderInstance();
    await doAsyncReplChecks(n, leader);

    console.log('killing leader %s', leader.endpoint);    
    await instanceManager.kill(leader);

    
    await instanceManager.asyncReplicationLeaderSelected(uuid);
    leader = await instanceManager.asyncReplicationLeaderInstance();
    // checks expecting one server less
    await doAsyncReplChecks(n - 1, leader);
    
    await instanceManager.restart(leader);
    console.log('killed instance restarted');

    //checks with one more server
    await doAsyncReplChecks(n, leader);
  });*/

});
