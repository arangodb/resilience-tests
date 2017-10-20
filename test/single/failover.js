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

  for (let i = 2; i <= 8; i *= 2) {
    it(`for ${i} servers`, async function() {
      await instanceManager.startSingleServer('single', i);
      await instanceManager.waitForAllInstances();

      // current leader
      let leaderUUID = await instanceManager.asyncReplicationLeaderSelected();
      expect(leaderUUID).to.not.equal(false);
      const leader = await instanceManager.asyncReplicationLeaderInstance();
      
      console.log("Leader selected, waiting for tick synchronization...");
      const inSync = await instanceManager.asyncReplicationTicksInSync();
      expect(inSync).to.equal(true, "slaves did not get in sync before timeout");

      console.log("Checking endpoints...");
      /// make sure all servers know the leader
      let servers = instanceManager.singleServers();
      expect(servers).to.have.lengthOf(i);
      for (let x = 0; x < servers.length; x++) {
        let list = await requestEndpoints(servers[x].endpoint);
        //expect(list).to.have.lengthOf(i);
        console.log("Endpoints %s", JSON.stringify(list));
        expect(leader.endpoint).to.equal(list[0]);
      }
    });
  }

  /*
  it('singles should have the same tick', async function() {
    await instanceManager.startSingleServer('single', 5);
    await instanceManager.waitForAllInstances();
    await instanceManager.asyncReplicationLeaderSelected(5);

    await sleep(30*1000); // /_api/cluster/endpoints returns all endpoints

    console.log(instanceManager.instances.map(inst=>inst.endpoint));

    console.log('beginne abschießen');
    for (let i = 0; i < 100; i++) {
      const masterInstance = await instanceManager.asyncReplicationMasterInstance(5);
      await instanceManager.kill(masterInstance);
      console.log('killed master');

      await sleep(10*1000);
      await instanceManager.asyncReplicationLeaderSelected(5);
      console.log('leader selected');

      await instanceManager.restart(masterInstance);
      console.log('master instance restarted');
      
      await sleep(20*1000);
      
      await instanceManager.asyncReplicationLeaderSelected(5);
      console.log('leader selected');

      const inSync = await instanceManager.asyncReplicationInSync(5);
      console.log('in sync', inSync);

      await sleep(30*1000);
    }

    const inSync = await instanceManager.asyncReplicationInSync(5);

    expect(inSync).to.equal(true);
  });*/

});
//*/