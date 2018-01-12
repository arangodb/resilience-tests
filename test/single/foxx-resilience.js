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

describe('Foxx service survive', async function() {

  const instanceManager = new InstanceManager('setup');
  
  beforeEach(async function(){
    await instanceManager.startAgency({agencySize:1});        
  });

  afterEach(function() {
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup();
  });

  for (let n = 2; n <= 8; n *= 2) { 
    let f = n / 2;    
    it.only(`for ${n} servers with ${f} failover, no restart`, async function() {
      await instanceManager.startSingleServer('single', n);
      await instanceManager.waitForAllInstances();

      // wait for leader selection
      let uuid = await instanceManager.asyncReplicationLeaderSelected();
      let leader = await instanceManager.asyncReplicationLeaderInstance();
      for (; f > 0; f--) {
        await doServerChecks(n, leader);
        // leader should not change
        expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);

        console.log('killing leader %s', leader.endpoint);    
        await instanceManager.kill(leader);
    
        uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
        leader = await instanceManager.asyncReplicationLeaderInstance();
        // checks expecting one server less
        await doServerChecks(--n, leader);
      }
    });
  }
  
});



// TODO add acutal checks for endpoints combined
// with health status from supervision. Problematic is the unclear
// delay between killing servers and a status update in Supervision/Health
/*async function doHealthChecks(n, leader) {
  // wait at least 0.5s + 2.5s for agency supervision
  // to persist the health status
  await sleep(5000);    

  const [info] = await instanceManager.rpAgency({
    method: 'POST',
    uri: baseUrl + '/_api/agency/read',
    json: true,
    body: [['/arango/Supervision/Health']]
  });

  let running = instanceManager.singleServers().filter(inst => inst.status === 'RUNNING');    
  let registered = info.arango.Target.Supervision.Health;
  Object.keys(registered).forEach(async uuid => {
    if (registered[uuid].Status === 'FAILED') {
      return;
    }
    const remote = instanceManager.resolveUUID(uuid);
    expect(running.find(ii => ii.endpoint === inst.endpoint)).to.be.not(undefined);

    let list = await requestEndpoints(remote.endpoint);
    expect(list).to.have.lengthOf(n, "Endpoints: " + JSON.stringify(list));     
    expect(leader.endpoint).to.equal(list[0]);      
  });
}*/
