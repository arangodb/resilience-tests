/* global describe, it, beforeEach, afterEach */
"use strict";
const InstanceManager = require("../InstanceManager.js");
const endpointToUrl = require("../common.js").endpointToUrl;

const expect = require("chai").expect;
const rp = require("request-promise");

let agencyRequest = function(options) {
  options.followRedirects = options.followRedirects || false;
  return rp(options)
    .then(response => {
      return response;
    })
    .catch(err => {
      if (err.statusCode === 307) {
        options.url = err.response.headers["location"];
        return agencyRequest(options);
      }
      return Promise.reject(err);
    });
};

let writeData = function(leader, data) {
  return agencyRequest({
    method: "POST",
    url: endpointToUrl(leader.endpoint) + "/_api/agency/write",
    json: true,
    body: [[data]],
    followRedirects: false
  });
};

let waitForReintegration = function(endpoint) {
  // mop: when reading works again we are reintegrated :)
  return rp({
    method: "POST",
    url: endpointToUrl(endpoint) + "/_api/agency/read",
    json: true,
    body: [["/"]],
    followRedirects: false
  }).catch(err => {
    if (err.statusCode == 307) {
      return Promise.resolve();
    } else {
      return waitForReintegration(endpoint);
    }
  });
};

let waitForLeaderChange = function(oldLeaderEndpoint, followerEndpoint) {
  let tries = 0;
  let maxTries = 200;
  let waitInterval = 50;

  let waitForLeaderChangeInner = function() {
    if (tries++ > maxTries) {
      return Promise.reject(
        "Didn't find an updated leader after " + tries + " tries"
      );
    }
    return rp({
      url: endpointToUrl(followerEndpoint) + "/_api/agency/config",
      json: true
    }).then(res => {
      let currentLeaderEndpoint = res.configuration.pool[res.leaderId];
      if (currentLeaderEndpoint == oldLeaderEndpoint) {
        return new Promise((resolve, reject) => {
          setTimeout(resolve, waitInterval);
        }).then(() => {
          return waitForLeaderChangeInner();
        });
      } else {
        return Promise.resolve();
      }
    });
  };
  return waitForLeaderChangeInner();
};

let waitForLeader = function(agents) {
  return rp({
    url: endpointToUrl(agents[0].endpoint) + "/_api/agency/config",
    json: true
  })
    .then(result => {
      if (result.leaderId === "") {
        return Promise.reject(new Error("no leader"));
      }
      return result.leaderId;
    })
    .catch(() => {
      return new Promise((resolve, reject) => {
        setTimeout(resolve, 100);
      }).then(() => {
        return waitForLeader(agents);
      });
    });
};

let getLeaderInstance = function(agents) {
  return waitForLeader(agents).then(leaderId => {
    return agents.reduce((leaderInstance, agent) => {
      if (leaderInstance) {
        return leaderInstance;
      }

      return agencyRequest({
        url: agent.endpoint + "/_api/agency/config",
        json: true
      }).then(result => {
        if (result.configuration.id === leaderId) {
          return agent;
        }
      });
    });
  });
};

describe("Agency", function() {
  let instanceManager = new InstanceManager("agency");
  let leader;
  let followers;

  describe("Agency startup", function() {
    let checkDataLoss = function(agentCount) {
      return instanceManager
        .startAgency({ agencySize: agentCount, agencyWaitForSync: true })
        .then(agents => {
          let data = { hans: "wurst" };
          return getLeaderInstance(agents)
            .then(leaderInstance => {
              return writeData(leaderInstance, data).then(() => {
                return leaderInstance;
              });
            })
            .then(leaderInstance => {
              return agencyRequest({
                method: "POST",
                url: endpointToUrl(leaderInstance.endpoint) +
                  "/_api/agency/read",
                json: true,
                body: [["/"]]
              });
            })
            .then(result => {
              expect(result).to.be.instanceof(Array);
              expect(result[0]).to.eql(data);
            })
            .then(() => {
              return Promise.all(
                agents.map(agent => instanceManager.kill(agent))
              );
            })
            .then(() => {
              return Promise.all(
                agents.map(agent => instanceManager.restart(agent))
              );
            })
            .then(() => {
              return getLeaderInstance(agents);
            })
            .then(leaderInstance => {
              return agencyRequest({
                method: "POST",
                url: endpointToUrl(leaderInstance.endpoint) +
                  "/_api/agency/read",
                json: true,
                body: [["/"]]
              });
            })
            .then(result => {
              expect(result).to.be.instanceof(Array);
              expect(result[0]).to.eql(data);
            });
        });
    };

    it("should not lose data upon restart when started in resilient mode", function() {
      return checkDataLoss(3);
    });

    it("should not lose data upon restart when started in single mode", function() {
      return checkDataLoss(1);
    });

    afterEach(function() {
      return instanceManager.cleanup().then(log => {
        if (this.currentTest.state === "failed") {
          this.currentTest.err.message =
            log + "\n\n" + this.currentTest.err.message;
        }
      });
    });
  });

  describe("Agency checks", function() {
    beforeEach(function() {
      // mop: without wait for sync we cannot trust the agency when it said it wrote everything
      // and we are doing tests to verify this behaviour here
      return instanceManager
        .startAgency({ agencySize: 3, agencyWaitForSync: true })
        .then(agents => {
          return getLeaderInstance(agents).then(leaderInstance => {
            leader = leaderInstance;
            followers = instanceManager
              .agents()
              .filter(agent => agent !== leader);
          });
        });
    });

    afterEach(function() {
      return instanceManager.cleanup().then(log => {
        if (this.currentTest.state === "failed") {
          this.currentTest.err.message =
            log + "\n\n" + this.currentTest.err.message;
        }
      });
    });

    it("should failover when stopping the leader", function() {
      let data = { hans: "wurst" };
      return writeData(leader, data)
        .then(() => {
          return instanceManager.kill(leader);
        })
        .then(() => {
          return waitForLeaderChange(leader.endpoint, followers[0].endpoint);
        })
        .then(res => {
          return agencyRequest({
            method: "POST",
            url: endpointToUrl(followers[0].endpoint) + "/_api/agency/read",
            json: true,
            body: [["/"]]
          });
        })
        .then(result => {
          expect(result).to.be.instanceof(Array);
          expect(result[0]).to.eql(data);
        });
    });

    it("should not think it is the leader after a restart", function() {
      let data = { hans: "wurst" };
      return writeData(leader, data)
        .then(() => {
          return instanceManager.kill(leader);
        })
        .then(() => {
          return instanceManager.restart(leader);
        })
        .then(() => {
          let upButNotLeader = function() {
            return rp({
              method: "POST",
              url: endpointToUrl(leader.endpoint) + "/_api/agency/read",
              json: true,
              body: [["/"]],
              followRedirects: false
            }).then(
              () => {
                return Promise.all([
                  rp({
                    url: endpointToUrl(leader.endpoint) + "/_api/agency/config"
                  }),
                  rp({
                    url: endpointToUrl(followers[0].endpoint) +
                      "/_api/agency/config"
                  })
                ]).then(results => {
                  throw new Error(
                    "It should not report success! It should block all incoming rest requests until it redetermined who the leader is. Configresults: " +
                      JSON.stringify({
                        leader: results[0],
                        follower: results[1]
                      })
                  );
                });
              },
              err => {
                if (err.statusCode == 503) {
                  // retry immediately...we want to find errors and not grant 1s grace time
                  return upButNotLeader();
                }
                expect(err.statusCode).to.equal(307);
              }
            );
          };

          return upButNotLeader();
        });
    });
    it("should reintegrate a crashed follower", function() {
      let data = { koeln: "sued" };
      return writeData(leader, data)
        .then(() => {
          return instanceManager.kill(followers[0], "SIGKILL");
        })
        .then(() => {
          return instanceManager.restart(followers[0]);
        })
        .then(() => {
          return waitForReintegration(followers[0].endpoint);
        })
        .then(() => {
          return agencyRequest({
            method: "POST",
            url: endpointToUrl(followers[0].endpoint) + "/_api/agency/read",
            json: true,
            body: [["/"]]
          });
        })
        .then(result => {
          expect(result).to.be.instanceof(Array);
          expect(result[0]).to.eql(data);
        });
    });
    it("should have the correct results after a funny fail rotation", function() {
      let retryUntilUp = function(fn) {
        let retries = 0;
        let retryUntilUpInner = function(fn) {
          let waitTime = 50;
          let maxRetries = 100;
          return fn().then(
            result => {
              return result;
            },
            err => {
              if (retries++ > maxRetries) {
                return Promise.reject(
                  "Couldn't find leader after " + retries + " retries"
                );
              } else if (err.statusCode == 503) {
                return new Promise((resolve, reject) => {
                  setTimeout(function() {
                    retryUntilUpInner(fn).then(resolve, reject);
                  }, waitTime);
                });
              } else {
                return Promise.reject(err);
              }
            }
          );
        };
        return retryUntilUpInner(fn);
      };
      let promise = Promise.resolve();
      for (let i = 0; i < instanceManager.instances.length * 2; i++) {
        promise = (function(promise, i) {
          return promise.then(() => {
            let data = { subba: { op: "increment" } };
            let instance =
              instanceManager.instances[i % instanceManager.instances.length];

            return retryUntilUp(function() {
              return writeData(instance, data);
            })
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
          return retryUntilUp(function() {
            return agencyRequest({
              method: "POST",
              url: endpointToUrl(leader.endpoint) + "/_api/agency/read",
              json: true,
              body: [["/"]]
            });
          });
        })
        .then(result => {
          expect(result).to.be.instanceof(Array);
          expect(result[0]).to.eql({
            subba: instanceManager.instances.length * 2
          });
        });
    });
    it("should reintegrate a failed follower starting with a new endpoint", function() {
      return instanceManager
        .kill(followers[0])
        .then(() => {
          return instanceManager.assignNewEndpoint(followers[0]);
        })
        .then(() => {
          return instanceManager.restart(followers[0]);
        })
        .then(() => {
          return waitForReintegration(followers[0].endpoint)
            .then(() => {
              return rp({
                url: endpointToUrl(followers[0].endpoint) +
                  "/_api/agency/config",
                json: true
              });
            })
            .then(result => {
              expect(result.leaderId).to.not.be.empty;
              expect(
                result.configuration.pool[result.configuration.id]
              ).to.equal(followers[0].endpoint);
              return result.configuration.id;
            });
        })
        .then(followerId => {
          return rp({
            url: endpointToUrl(leader.endpoint) + "/_api/agency/config",
            json: true
          }).then(result => {
            expect(result.configuration.pool[followerId]).to.equal(
              followers[0].endpoint
            );
          });
        });
    });
    it("should reintegrate a failed leader starting with a new endpoint", function() {
      return instanceManager
        .kill(leader)
        .then(() => {
          return instanceManager.assignNewEndpoint(leader);
        })
        .then(() => {
          return instanceManager.restart(leader);
        })
        .then(() => {
          return waitForReintegration(leader.endpoint)
            .then(() => {
              return rp({
                url: endpointToUrl(leader.endpoint) + "/_api/agency/config",
                json: true
              });
            })
            .then(result => {
              expect(result.leaderId).to.not.be.empty;
              expect(
                result.configuration.pool[result.configuration.id]
              ).to.equal(leader.endpoint);
              return result.configuration.id;
            });
        })
        .then(oldLeaderId => {
          return rp({
            url: endpointToUrl(followers[0].endpoint) + "/_api/agency/config",
            json: true
          }).then(result => {
            expect(result.configuration.pool[oldLeaderId]).to.equal(
              leader.endpoint
            );
          });
        });
    });
  });
});
