'use strict';
const router = require('@arangodb/foxx/router')();
module.context.use(router);
router.get((req, res) => {
  res.send('service1');
})
.response(['text/plain']);

