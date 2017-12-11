'use strict';
const spawn = require('child_process').spawn;
const portfinder = require('portfinder');
const ip = require('ip');

function startInstance(instance) {
  instance.port = portFromEndpoint(instance.endpoint);
  return new Promise((resolve, reject) => {
    console.error(instance.name, "STARTUP");
    const process = spawn(instance.binary, instance.args);

    process.stdout.on('data', data =>
      data.toString().split('\n').forEach(instance.logFn));

    process.stderr.on('data', data =>
      data.toString().split('\n').forEach(instance.logFn));

    process.on('exit', code => {
      console.error(instance.name, "EXITED");
      instance.status = 'EXITED';
      instance.exitcode = code;

      // instance.logFn(`exited with code ${code}`);
    });
    console.error(instance.name, "IS NOW RUNNING");
    instance.exitcode = null;
    instance.status = 'RUNNING';
    instance.process = process;
    resolve(instance);
  });
}

let startMinPort = 4000;
if (process.env.MIN_PORT) {
  startMinPort = parseInt(process.env.MIN_PORT, 10);
}

let portOffset = 50;
if (process.env.PORT_OFFSET) {
  portOffset = parseInt(process.env.PORT_OFFSET, 10);
}

let maxPort = 65535;
if (process.env.MAX_PORT) {
  maxPort = parseInt(process.env.MAX_PORT, 10);
}
let minPort = startMinPort;
let findFreePort = function(ip) {
  let startPort = minPort;
  minPort += portOffset;
  if (minPort >= maxPort) {
    minPort = startMinPort;
  }
  return portfinder.getPortPromise({ port: startPort, host: ip})
};

function portFromEndpoint(endpoint) {
  return endpoint.match(/:(\d+)\/?/)[1];
}

function createEndpoint() {
  let myIp = ip.address();

  return findFreePort(myIp).then(port => {
    return 'tcp://' + myIp + ':' + port;
  });
}

const endpointToUrl = function(endpoint) {
  if (endpoint.substr(0, 6) === 'ssl://') {
    return 'https://' + endpoint.substr(6);
  }

  const pos = endpoint.indexOf('://');

  if (pos === -1) {
    return 'http://' + endpoint;
  }

  return 'http' + endpoint.substr(pos);
};

const compareTicks = function(l, r) {
  var i;
  if (l === null) {
    l = "0";
  }
  if (r === null) {
    r = "0";
  }
  if (l.length !== r.length) {
    return l.length - r.length < 0 ? -1 : 1;
  }

  // length is equal
  for (i = 0; i < l.length; ++i) {
    if (l[i] !== r[i]) {
      return l[i] < r[i] ? -1 : 1;
    }
  }

  return 0;
};

exports.endpointToUrl = endpointToUrl;
exports.portFromEndpoint = portFromEndpoint;
exports.createEndpoint = createEndpoint;
exports.startInstance = startInstance;
exports.findFreePort = findFreePort;
exports.compareTicks = compareTicks;
