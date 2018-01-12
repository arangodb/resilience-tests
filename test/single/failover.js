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

describe('Testing leader-followe failover', async function() {

  const instanceManager = new InstanceManager('setup');
  
  beforeEach(async function(){
    await instanceManager.startAgency({agencySize:1});        
  });

  afterEach(function() {
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup();
  });

  describe('Synchronize tick values', async function() {
  
    /// check tick values synchronize, check endpoints
    /// TODO check for redirects to leader
    async function doServerChecks(n, leader) {
      console.log("Waiting for tick synchronization...");
      const inSync = await instanceManager.asyncReplicationTicksInSync(60.0);
      expect(inSync).to.equal(true, "followers did not get in sync before timeout");   
  
      console.log("Checking endpoints...");
      /// make sure all servers know the leader
      let servers = instanceManager.singleServers().filter(inst => inst.status === 'RUNNING');
      expect(servers).to.have.lengthOf(n);
      for (let x = 0; x < servers.length; x++) {
        let url = endpointToUrl(servers[x].endpoint);
        let body = await rp.get({ uri: `${url}/_admin/server/role`, json: true});
        expect(body.mode).to.equal("resilient", `Wrong response ${JSON.stringify(body)}`);
        //  TODO check location header on other APIs
  
        let list = await requestEndpoints(servers[x].endpoint);
        console.log("Endpoints list: %s", JSON.stringify(list));
        console.log("expected: %s", JSON.stringify(leader.endpoint));
        expect(list[0]).to.have.property("endpoint");
        expect(list[0].endpoint).to.equal(leader.endpoint);
        // Could also check for presence of all follower endpoints,
        // but due to lag of the supervision these might only turn up
        // 60s later, which would prolong these tests
      }
    }
  
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
  
    [2, 4, 8].forEach(n => {
    it(`for ${n} servers`, async function() {
        await instanceManager.startSingleServer('single', n);
        await instanceManager.waitForAllInstances();
  
        // get current leader
        let uuid = await instanceManager.asyncReplicationLeaderSelected();
        console.log("Leader selected");
        const leader = await instanceManager.asyncReplicationLeaderInstance();
  
        await doServerChecks(n, leader);
  
        // leader should not change
        expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);
      });
    });
  
    [2, 4].forEach(n => {
      let f = n / 2;
      it(`for ${n} servers with ${f} failover, leader restart`, async function() {
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
          let old = leader;
      
          uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
          leader = await instanceManager.asyncReplicationLeaderInstance();
          // checks expecting one server less
          await doServerChecks(n - 1, leader);
          
          await instanceManager.restart(old);
          console.log('killed instance restarted');
        }
      });
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
  
  describe('Replicating data', async function() {
  
    async function generateData(db, num) {
      let coll = await db.collection("testcollection");
      await coll.create();
      return Promise.all(Array.apply(0, Array(num))
            .map( (x, i) =>  i)
            .map(i => coll.save({test:i})));
    }
  
    async function checkData(db, num) {
      let cursor = await db.query(`FOR x IN testcollection 
                                    SORT x.test ASC RETURN x`);
      expect(cursor.hasNext()).to.equal(true);
      let i = 0;
      while (cursor.hasNext()) {
        let doc = await cursor.next();
        expect(doc.test).to.equal(i++);
      }
      expect(i).to.equal(num);
    }
  
    [100, 1000, 25000].forEach(numDocs => {
      for (let n = 2; n <= 8; n *= 2) {
        let f = n / 2;
        it(`with ${n} servers, ${f} failover, leader restart ${numDocs}`, async function() {
          await instanceManager.startSingleServer('single', n);
          await instanceManager.waitForAllInstances();
    
          // wait for leader selection
          let uuid = await instanceManager.asyncReplicationLeaderSelected();
          let leader = await instanceManager.asyncReplicationLeaderInstance();
    
          let db = arangojs({ url: endpointToUrl(leader.endpoint), databaseName: '_system' });
          await generateData(db, numDocs);
    
          for (; f > 0; f--) {
            console.log("Waiting for tick synchronization...");
            const inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
            expect(inSync).to.equal(true, "followers did not get in sync before timeout");  
    
            // leader should not change
            expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);
    
            console.log('killing leader %s', leader.endpoint);    
            await instanceManager.kill(leader);
            let old = leader;
        
            uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
            leader = await instanceManager.asyncReplicationLeaderInstance();
    
            // we need to wait for the server to get out of maintenance mode
            await sleep(250); // TODO do not use sleep here ?
            db = arangojs({ url: endpointToUrl(leader.endpoint), databaseName: '_system' });
            await checkData(db, numDocs);
            
            await instanceManager.restart(old);
            console.log('killed instance restarted');
          }
        });
      }
    });
  
    [100, 1000, 25000].forEach(numDocs => {
      for (let n = 2; n <= 8; n *= 2) {
        let f = n - 1;
        it(`with ${n} servers, ${f} failover, no restart ${numDocs}`, async function() {
          await instanceManager.startSingleServer('single', n);
          await instanceManager.waitForAllInstances();
    
          // wait for leader selection
          let uuid = await instanceManager.asyncReplicationLeaderSelected();
          let leader = await instanceManager.asyncReplicationLeaderInstance();
    
          let db = arangojs({ url: endpointToUrl(leader.endpoint), databaseName: '_system' });
          await generateData(db, numDocs);
    
          for (; f > 0; f--) {
            console.log("Waiting for tick synchronization...");
            const inSync = await instanceManager.asyncReplicationTicksInSync(120.0);
            expect(inSync).to.equal(true, "followers did not get in sync before timeout");  
    
            // leader should not change
            expect(await instanceManager.asyncReplicationLeaderId()).to.equal(uuid);
    
            console.log('killing leader %s', leader.endpoint);    
            await instanceManager.kill(leader);
        
            uuid = await instanceManager.asyncReplicationLeaderSelected(uuid);
            leader = await instanceManager.asyncReplicationLeaderInstance();
    
            // we need to wait for the server to get out of maintenance mode
            await sleep(250); // TODO do not use sleep here ?
            db = arangojs({ url: endpointToUrl(leader.endpoint), databaseName: '_system' });
            await checkData(db, numDocs);
          }
        });
      }
    });
  });  
});

