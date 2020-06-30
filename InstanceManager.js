"use strict";
const aim = require("arangodb-instance-manager");

exports.create = () => {
  let pathOrImage;
  let runner;
  if (process.env.RESILIENCE_ARANGO_BASEPATH) {
    pathOrImage = process.env.RESILIENCE_ARANGO_BASEPATH;
    runner = "local";
  } else if (process.env.RESILIENCE_DOCKER_IMAGE) {
    pathOrImage = process.env.RESILIENCE_DOCKER_IMAGE;
    runner = "docker";
  }
  if (!runner) {
    throw new Error(
      'Must specify RESILIENCE_ARANGO_BASEPATH (source root dir including a "build" folder containing compiled binaries or RESILIENCE_DOCKER_IMAGE to test a docker container'
    );
  }

  let storageEngine = process.env.ARANGO_STORAGE_ENGINE === "mmfiles" ? "mmfiles"  : "rocksdb";
  return new aim.InstanceManager(pathOrImage, runner, storageEngine);
};

exports.endpointToUrl = aim.endpointToUrl;
exports.FailoverError = aim.FailoverError;
exports.waitForInstance = aim.InstanceManager.waitForInstance;
exports.rpAgency = aim.InstanceManager.rpAgency;
