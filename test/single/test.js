/* global describe, it, afterEach */
'use strict';

const InstanceManager = require('../../InstanceManager.js');
const rp = require('request-promise');
const arangojs = require('arangojs');
const expect = require('chai').expect;
const sleep = (ms= 1000) => new Promise(resolve => setTimeout(resolve, ms));

describe('SameTick', function() {
  const instanceManager = new InstanceManager('setup');
  it('all singles should have the same tick', async function() {
      await instanceManager.startAgency({agencySize:1});

      await instanceManager.startSingleServer('async-failover', 5);
      await instanceManager.waitForAllInstances();
      await instanceManager.asyncReplicationLeaderSelected(5);

      await sleep(30*1000); // /_api/cluster/endpoints returns all endpoints

      const inSync = await instanceManager.asyncReplicationInSync(5);

      expect(inSync).to.equal(true);
  });

  afterEach(function() {
    instanceManager.moveServerLogs(this.currentTest);
    return instanceManager.cleanup();
  });
});

















const f = async () => {
    const im = new (require('./InstanceManager'))();

    await im.startAgency({agencySize:1});

    await im.startSingleServer('async-failover', 5);
    await im.AsyncReplicationReady(5);


    await sleep(10*1000);
    const master = await im.AsyncReplicationMaster();
    console.log('master is', master);

    im.instances.forEach(inst => console.log(inst.endpoint));
    // let body = await rp.get({ uri: `${endpointToUrl(insts[0].endpoint)}/_api/cluster/endpoints`, json: true});
    // console.log(body);






    // db._connection.request({url:'',
    // body: ,
    // method:'PUT',
    // headers: }, function(err, res) {
    //     console.log(res.statusCode);
    //     console.log(res.headers);
    //     console.log(res.bod);
    // });

    // req.put({path:'/_db/_system/_api/replication/make-slave?global=true',
    //             buffer:Buffer.from(JSON.stringify({endpoint: 'tcp://192.168.173.188:4150', autoStart: true, includeSystem: true})),
    //         headers: { 'X-Arango-Async': 'store'}}, (status, headers, body) => {
                
    //             console.log(status);
    //             console.log(headers);
    //             console.log(body.toString());
    // });


    // const master = await im.startSingleServer('master');
    // const slave  = await im.startSingleServer('slave');
    // console.log(master.endpoint);
    // console.log(slave.endpoint);



};

// f().catch(e => console.log(e));
