'use strict';

const path = require('path');
const fs = require('fs');
const FoxxService = require('@arangodb/foxx/service');

const router = require('@arangodb/foxx/router')();

router
  .head((req, res) => {
    res.statusCode = 404;
    const mount = req.queryParams.mount;
    if (mount) {
      const bundlePath = FoxxService.bundlePath(mount);
      const basePath = FoxxService.basePath(mount);
      if (fs.exists(bundlePath) && fs.exists(basePath)) {
        res.statusCode = 200;
      }
    }
  })
  .queryParam('mount');

router
  .delete((req, res) => {
    const mount = req.queryParams.mount;
    if (mount) {
      const bundlePath = FoxxService.bundlePath(mount);
      if (fs.exists(bundlePath)) {
        fs.remove(bundlePath);
      }
      const basePath = FoxxService.basePath(mount);
      if (fs.exists(basePath)) {
        fs.removeDirectoryRecursive(basePath, true);
      }
    }
  })
  .queryParam('mount');

router
  .post((req, res) => {
    const filename = fs.getTempFile('util-manage', true);
    fs.writeFileSync(filename, req.body);

    const mount = req.queryParams.mount;
    const bundlePath = FoxxService.bundlePath(mount);
    if (fs.exists(bundlePath)) {
      fs.remove(bundlePath);
    }
    fs.makeDirectoryRecursive(path.dirname(bundlePath));
    fs.move(filename, bundlePath);
    const basePath = FoxxService.basePath(mount);
    if (fs.exists(basePath)) {
      fs.removeDirectoryRecursive(basePath, true);
    }
    fs.makeDirectoryRecursive(basePath);
    fs.unzipFile(bundlePath, basePath, false, true);
  })
  .queryParam('mount');

router.get('/checksums', (req, res) => {
  const mountParam = req.queryParams.mount || [];
  const mounts = Array.isArray(mountParam) ? mountParam : [mountParam];
  const checksums = {};
  for (const mount of mounts) {
    try {
      checksums[mount] = FoxxService.checksum(mount);
    } catch (e) {}
  }
  res.json(checksums);
});

module.context.use(router);
