const spawn = require('child_process').spawn;
const portastic = require('portastic');
const ip = require('ip');

function startInstance(instance) {
  instance.port = portFromEndpoint(instance.endpoint);
  return new Promise((resolve, reject) => {
    let process = spawn(instance.binary, instance.args);

    process.stdout.on('data', data => {
      data.toString().split('\n').forEach(instance.logFn);
    });

    process.stderr.on('data', data => {
      data.toString().split('\n').forEach(instance.logFn);
    });
    process.on('close', code => {
      instance.status = 'EXITED';
      instance.exitcode = code;

      //instance.logFn(`exited with code ${code}`);
    });
    instance.exitcode = null;
    instance.status = 'RUNNING';
    instance.process = process;
    resolve(instance);
  });
}

let minPort = startMinPort = 3000;
let findFreePort = function(ip) {
  let startPort = minPort;
  minPort += 100;
  return portastic.find({min: startPort, max: 65000, retrieve: 1}, ip)
  .then(ports => {
    let port = ports[0];
    if (minPort > 64000) {
      minPort = startMinPort;
    }
    return port;
  });
}

function portFromEndpoint(endpoint) {
  return endpoint.match(/:(\d+)\/?/)[1];
}

function createEndpoint() {
  let myIp = ip.address();

  return findFreePort(myIp)
    .then(port => {
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
}

exports.endpointToUrl = endpointToUrl;
exports.portFromEndpoint = portFromEndpoint;
exports.createEndpoint = createEndpoint;
exports.startInstance = startInstance;
exports.findFreePort = findFreePort;
