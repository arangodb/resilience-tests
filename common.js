const spawn = require('child_process').spawn;
const portastic = require('portastic');

function startInstance(instance) {
  instance.port = portFromEndpoint(instance.endpoint);
  return new Promise((resolve, reject) => {
    let process = spawn(instance.binary, instance.args);
    let logFn = line => {
      if (line.trim().length > 0) {
        console.log(instance.name + '(' + process.pid + '): \t' + line);
      }
    }

    process.stdout.on('data', data => {
      data.toString().split('\n').forEach(logFn);
    });

    process.stderr.on('data', data => {
      data.toString().split('\n').forEach(logFn);
    });
    process.on('close', code => {
      instance.status = 'EXITED';
      instance.exitcode = code;
      logFn(`exited with code ${code}`);
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

exports.startInstance = startInstance;
exports.findFreePort = findFreePort;
