# ArangoDB resilience tests

This repo contains a testsuite to test some of ArangoDBs resilience capabilities.

Under the hood it is just a more or less standard mocha testsuite plus some stuff to keep arangodb running.

## Requirements

You need a pretty recent v7.6+ nodejs and npm and either a compiled ArangoDB source directory or a docker container you want to test

## Installation

`npm install` will install all required libraries. `yarn` should work too.

## Executing

Simply execute

`npm run test-jenkins`

This will bail out like this:

```
Error: Must specify RESILIENCE_ARANGO_BASEPATH (source root dir including a "build" folder containing compiled binaries or RESILIENCE_DOCKER_IMAGE to test a docker container
```

Specify the path to your arangodb source directory containing a `build` directory where you created an arangodb build.

Then reexecute like this (replace path of course):

`RESILIENCE_ARANGO_BASEPATH=../arangodb npm run test-jenkins`

## Options

RESILIENCE_ARANGO_BASEPATH

    Path to your arangodb source directory containing a build directory with arango executables. Example: "../arangodb"

RESILIENCE_DOCKER_IMAGE

    Docker image to test. Example: "arangdb/arangodb"

LOG_IMMEDIATE

    By default log output is being surpressed and only shown if there is an error. By setting this to 1 the logoutput will be thrown onto the console right away (useful for debugging)

ARANGO_STORAGE_ENGINE

    One of rocksdb or mmfiles (default: mmfiles)

PORT_OFFSET

    By default the resilience tests will take a port 40000, test it and then add 100 for the next instance. By specifying a PORT_OFFSET the start port is offsetted and this way you can prevent port clashes more easily if executed in parallel on a CI for example. (Example: PORT_OFFSET=2).

## Mocha options

The tests itself are run through mocha so you can append mocha commands to the `npm run` script as you would expect:

Example:

```
RESILIENCE_ARANGO_BASEPATH=../arangodb npm test -- --grep "Move shards"
```