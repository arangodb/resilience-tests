# ArangoDB resilience tests

This repo contains a testsuite to test some of ArangoDBs resilience capabilities.

Under the hood it is just a more or less standard mocha testsuite plus some stuff to keep arangodb running.

## Requirements

You need a pretty recent v7.6+ nodejs and yarn or npm and either a compiled ArangoDB source directory or a docker container you want to test

## Installation

`yarn` will install all required libraries. `npm install` should work too.

## Executing

Simply execute

`yarn test-jenkins` or `npm run test-jenkins`

This will bail out like this:

```
Error: Must specify RESILIENCE_ARANGO_BASEPATH (source root dir including a "build" folder containing compiled binaries or RESILIENCE_DOCKER_IMAGE to test a docker container
```

Specify the path to your arangodb source directory containing a `build` directory where you created an arangodb build.

Then reexecute like this (replace path of course):

`RESILIENCE_ARANGO_BASEPATH=../arangodb yarn test-jenkins`

## Options

via environment variables

### `RESILIENCE_ARANGO_BASEPATH`

    Path to your arangodb source directory containing a build directory with arango executables. Example: "../arangodb"

### `RESILIENCE_DOCKER_IMAGE`

    Docker image to test. Example: "arangdb/arangodb"

### `LOG_IMMEDIATE`

    Set to `1` for debug log output from the tests and the instance manager.

### `ARANGO_STORAGE_ENGINE`

    One of rocksdb or mmfiles (default: mmfiles)

### `MIN_PORT`

    From where the tests should start searching for a free port. defaults to 4000

### `MAX_PORT`

    MAX_PORT. defaults to 65535

### `PORT_OFFSET`

    Port offset. For every request this will be added to the startPort to keep the ports somewhat predicatable. Default 50
    The first request would reveal for example 4000. The second instance would then be assigned port 4050, then 4100 and so forth.

### `RESILIENCE_ARANGO_WRAPPER`

    Wrapper command for arangod, e.g. rr.

### `LOG_AGENCY`,
### `LOG_COMMUNICATION`,
### `LOG_REPLICATION`,
### `LOG_REQUESTS`

    Set log level for agency, communication, replication or requests,
    values are e.g. `debug` or `trace`.
    Will be passed to arangod appended to `--log.level=agency=`,
    `--log.level=communication=` etc..

### `ARANGO_INSTANCES_DIR`

    Directory where the instance manager creates its instances. Useful for
    collecting artifacts after failed tests. May be relative to the current
    working directory. Will be created if non existent, but not recursively.
    If unset, the systems tmp directory will be used.

### `AGENCY_COMPACTION_STEP`

    Will be passed via `--agency.compaction-step-size=` to arangod. Defaults
    to 200.

### `AGENCY_COMPACTION_KEEP`

    Will be passed via `--agency.compaction-keep-size=` to arangod. Defaults
    to 100.

### `ARANGO_EXTRA_ARGS`

    Extra arguments that will be passed to arangod. Will be split on spaces
    and then passed as separate arguments.

## Mocha options

The tests itself are run through mocha so you can append mocha commands to the `package.json` script as you would expect:

Some Examples:

```
RESILIENCE_ARANGO_BASEPATH=../arangodb yarn test -- --grep "Move shards"
RESILIENCE_ARANGO_BASEPATH=../arangodb ARANGO_STORAGE_ENGINE=rocksdb yarn test -- test/cluster/shard-move.js
```
