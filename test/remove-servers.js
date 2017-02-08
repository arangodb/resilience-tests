/* global describe, it, before, after */
'use strict';
const InstanceManager = require('../InstanceManager.js');
const expect = require('chai').expect;
const arangojs = require('arangojs');
const rp = require('request-promise');


describe('Remove servers', function () {
  let instanceManager = new InstanceManager('remove servers');
  let db;

  let waitForFailedHealth = function(serverEndpoint, maxTime) {
    let coordinator = instanceManager.coordinators().filter(server => server.status == 'RUNNING')[0];
    return rp({
      url: instanceManager.getEndpointUrl(coordinator) + '/_admin/cluster/health',
      json: true,
    })
    .then(health => {
      health = health.Health;
      let serverId = Object.keys(health).filter(serverId => {
        return health[serverId].Endpoint == serverEndpoint;
      })[0];

      if (serverId === undefined) {
        return Promise.reject(new Error('Couldn\'t find a server in health struct'));
      } else {
        return health[serverId];
      }
    })
    .then(healthServer => {
      if (healthServer.Status != 'FAILED') {
        if (maxTime > Date.now()) {
          return new Promise((resolve, reject) => {
            setTimeout(resolve, 100);
          })
          .then(() => {
            return waitForFailedHealth(serverEndpoint, maxTime);
          });
        } else {
          return Promise.reject(new Error('Server did not go failed in time!'));
        }
      }
      return true;
    });
  }

  afterEach(function () {
    if (this.currentTest.state === 'failed') {
      this.currentTest.err.message = instanceManager.currentLog + '\n\n' + this.currentTest.err.message;
    }
    instanceManager.currentLog = '';
    return instanceManager.cleanup();
  });
  it('should mark a failed coordinator failed after a while', function() {
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      let coordinator = instanceManager.coordinators()[1];
      return instanceManager.kill(coordinator)
      .then(() => {
        return coordinator;
      })
    })
    .then(coordinator => {
      return waitForFailedHealth(coordinator.endpoint, Date.now() + 60000)
    });
  });
  it('should not be possile to remove a running coordinator', function() {
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/health',
        json: true,
      });
    })
    .then(health => {
      health = health.Health;
      let serverId = Object.keys(health).filter(serverId => {
        return health[serverId].Role == "Coordinator";
      })[0];
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/removeServer',
        json: true,
        method: 'post',
        body: serverId,
      });
    })
    .then(() => {
      return Promise.reject(new Error('What? Removing a server that is active should not be possible'));
    }, err => {
      expect(err.statusCode).to.eql(412);
    })
  });
  it('should raise a proper error when removing a non existing server', function() {
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/removeServer',
        json: true,
        method: 'post',
        body: 'der hund',
      });
    })
    .then(() => {
      return Promise.reject(new Error('What? Removing a non existing server should not be possible'));
    }, err => {
      expect(err.statusCode).to.eql(404);
    })
  });
  it('should be able to remove a failed coordinator', function() {
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      let coordinator = instanceManager.coordinators()[1];
      return instanceManager.kill(coordinator)
      .then(() => {
        return coordinator;
      })
    })
    .then(coordinator => {
      return waitForFailedHealth(coordinator.endpoint, Date.now() + 60000)
      .then(() => {
        return coordinator;
      });
    })
    .then(coordinator => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/health',
        json: true,
      })
      .then(health => {
        health = health.Health;
        let serverId = Object.keys(health).filter(serverId => {
          return health[serverId].Endpoint == coordinator.endpoint;
        })[0];
        return serverId;
      })
    })
    .then(serverId => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/removeServer',
        json: true,
        method: 'post',
        body: serverId,
      });
    });
  });
  it('should not remove a failed dbserver (yet)', function() {
    return instanceManager.startCluster(1, 2, 2)
    .then(() => {
      let dbserver = instanceManager.dbServers()[0];
      return instanceManager.kill(dbserver)
      .then(() => {
        return dbserver;
      })
    })
    .then(dbserver => {
      return waitForFailedHealth(dbserver.endpoint, Date.now() + 60000)
      .then(() => {
        return dbserver;
      });
    })
    .then(dbserver => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/health',
        json: true,
      })
      .then(health => {
        health = health.Health;
        let serverId = Object.keys(health).filter(serverId => {
          return health[serverId].Endpoint == dbserver.endpoint;
        })[0];
        return serverId;
      })
    })
    .then(serverId => {
      return rp({
        url: instanceManager.getEndpointUrl() + '/_admin/cluster/removeServer',
        json: true,
        method: 'post',
        body: serverId,
      });
    })
    .then(() => {
      return Promise.reject(new Error('What? Removing a dbserver should not be possible'));
    }, err => {
      expect(err.statusCode).to.eql(400);
    })
  });
});
