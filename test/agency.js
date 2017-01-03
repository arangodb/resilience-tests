/* global describe, it, beforeEach, afterEach */
'use strict';
const InstanceManager = require('../InstanceManager.js');
const endpointToUrl = require('../common.js').endpointToUrl;

const expect = require('chai').expect;
const rp = require('request-promise');

let agencyRequest = function (options) {
  options.followRedirects = options.followRedirects || false;
  return rp(options)
  .then(response => {
    return response;
  })
  .catch(err => {
    if (err.statusCode === 307) {
      options.url = err.response.headers['location'];
      return agencyRequest(options);
    }
    return Promise.reject(err);
  });
};

let writeData = function (leader, data) {
  return agencyRequest({
    method: 'POST',
    url: endpointToUrl(leader.endpoint) + '/_api/agency/write',
    json: true,
    body: [[data]],
    followRedirects: false
  });
};

describe('Agency', function () {
  let instanceManager = new InstanceManager('agency');
  let leader;
  let followers;

  beforeEach(function () {
    return instanceManager.startAgency({agencySize: 3})
    .then(agents => {
      let checkForLeader = function () {
        return rp({
          url: endpointToUrl(agents[0].endpoint) + '/_api/agency/config',
          json: true
        })
        .then(result => {
          if (result.leaderId === '') {
            return Promise.reject();
          }
          return result.leaderId;
        })
        .catch(() => {
          return new Promise((resolve, reject) => {
            setTimeout(resolve, 100);
          })
          .then(() => {
            return checkForLeader();
          });
        });
      };
      return checkForLeader()
      .then(leaderId => {
        return agents.reduce((leaderInstance, agent) => {
          if (leaderInstance) {
            return leaderInstance;
          }

          return agencyRequest({
            url: agent.endpoint + '/_api/agency/config',
            json: true
          })
          .then(result => {
            if (result.configuration.id === leaderId) {
              return agent;
            }
          });
        });
      })
      .then(leaderInstance => {
        leader = leaderInstance;
        followers = instanceManager.agents().filter(agent => agent !== leader);
      });
    });
  });

  afterEach(function () {
    return instanceManager.cleanup()
    .then(log => {
      if (this.currentTest.state === 'failed') {
        this.currentTest.err.message = log + '\n\n' + this.currentTest.err.message;
      }
    });
  });

  it('should failover when stopping the leader', function () {
    let data = {'hans': 'wurst'};
    return writeData(leader, data)
    .then(() => {
      return instanceManager.kill(leader);
    })
    .then(() => {
      // mop: well the failover is of course not fully immediately
      return new Promise((resolve, reject) => {
        setTimeout(resolve, 100);
      });
    })
    .then(() => {
      return agencyRequest({
        method: 'POST',
        url: endpointToUrl(followers[0].endpoint) + '/_api/agency/read',
        json: true,
        body: [['/']]
      });
    })
    .then(result => {
      expect(result).to.be.instanceof(Array);
      expect(result[0]).to.eql(data);
    });
  });

  it('should not think it is the leader after a restart', function () {
    let data = {'hans': 'wurst'};
    return writeData(leader, data)
    .then(() => {
      return instanceManager.kill(leader);
    })
    .then(() => {
      return instanceManager.restart(leader);
    })
    .then(() => {
      return rp({
        method: 'POST',
        url: endpointToUrl(leader.endpoint) + '/_api/agency/read',
        json: true,
        body: [['/']],
        followRedirects: false
      })
      .then(() => {
        return Promise.all([
          rp({
            url: endpointToUrl(leader.endpoint) + '/_api/agency/config'
          }),
          rp({
            url: endpointToUrl(followers[0].endpoint) + '/_api/agency/config'
          })
        ])
        .then(results => {
          throw new Error('It should not report success! It should block all incoming rest requests until it redetermined who the leader is. Configresults: ' + JSON.stringify({leader: results[0], follower: results[1]}));
        });
      }, err => {
        expect(err.statusCode).to.equal(307);
      });
    });
  });
  it('should reintegrate a crashed follower', function () {
    let data = {'koeln': 'sued'};
    return writeData(leader, data)
    .then(() => {
      return instanceManager.kill(followers[0], 'SIGKILL');
    })
    .then(() => {
      return instanceManager.restart(followers[0]);
    })
    .then(() => {
      return agencyRequest({
        method: 'POST',
        url: endpointToUrl(followers[0].endpoint) + '/_api/agency/read',
        json: true,
        body: [['/']]
      });
    })
    .then(result => {
      expect(result).to.be.instanceof(Array);
      expect(result[0]).to.eql(data);
    });
  });
  it('should have the correct results after a funny fail rotation', function () {
    let promise = Promise.resolve();
    for (let i = 0; i < instanceManager.instances.length * 2; i++) {
      promise = (function (promise, i) {
        return promise.then(() => {
          let data = {'subba': i};
          let instance = instanceManager.instances[i % instanceManager.instances.length];
          return writeData(instance, data)
          .then(() => {
            return instanceManager.kill(instance);
          })
          .then(() => {
            return instanceManager.restart(instance);
          });
        });
      })(promise, i);
    }

    return promise
    .then(() => {
      return agencyRequest({
        method: 'POST',
        url: endpointToUrl(leader.endpoint) + '/_api/agency/read',
        json: true,
        body: [['/']]
      });
    })
    .then(result => {
      expect(result).to.be.instanceof(Array);
      expect(result[0]).to.eql({'subba': instanceManager.instances.length * 2 - 1});
    });
  });
  it('should reintegrate a failed follower starting with a new endpoint', function() {
    return instanceManager.kill(followers[0])
    .then(() => {
      return instanceManager.assignNewEndpoint(followers[0]);
    })
    .then(() => {
      return instanceManager.restart(followers[0]);
    })
    .then(() => {
      return rp({
        url: endpointToUrl(followers[0].endpoint) + '/_api/agency/config',
        json: true
      })
      .then(result => {
        expect(result.leaderId).to.not.be.empty;
        expect(result.configuration.pool[result.configuration.id]).to.equal(followers[0].endpoint);
        return result.configuration.id;
      })
    })
    .then(followerId => {
      return rp({
        url: endpointToUrl(leader.endpoint) + '/_api/agency/config',
        json: true
      })
      .then(result => {
        expect(result.configuration.pool[followerId]).to.equal(followers[0].endpoint);
      })
    })
  });
  it('should reintegrate a failed leader starting with a new endpoint', function() {
    return instanceManager.kill(leader)
    .then(() => {
      return instanceManager.assignNewEndpoint(leader);
    })
    .then(() => {
      return instanceManager.restart(leader);
    })
    .then(() => {
      return rp({
        url: endpointToUrl(leader.endpoint) + '/_api/agency/config',
        json: true
      })
      .then(result => {
        expect(result.leaderId).to.not.be.empty;
        expect(result.configuration.pool[result.configuration.id]).to.equal(leader.endpoint);
        return result.configuration.id;
      })
    })
    .then(oldLeaderId => {
      return rp({
        url: endpointToUrl(followers[0].endpoint) + '/_api/agency/config',
        json: true
      })
      .then(result => {
        expect(result.configuration.pool[oldLeaderId]).to.equal(leader.endpoint);
      })
    })
  });
});
