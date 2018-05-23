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
  let storageEngine;
  if (process.env.ARANGO_STORAGE_ENGINE) {
    storageEngine = process.env.ARANGO_STORAGE_ENGINE;
    if (storageEngine !== "rocksdb") {
      storageEngine = "mmfiles";
    }
  } else {
    storageEngine = "mmfiles";
  }
  return new aim.InstanceManager(pathOrImage, runner, storageEngine);
};

exports.endpointToUrl = aim.endpointToUrl;
exports.FailoverError = aim.FailoverError;
