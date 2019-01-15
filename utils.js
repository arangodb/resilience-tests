/*jshint globalstrict:true, strict:true, esnext: true */

"use strict";

////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Tobias Gödderz
////////////////////////////////////////////////////////////////////////////////

const dd = require('dedent');

const sleep = (ms = 1000) => new Promise(resolve => setTimeout(resolve, ms));

const debugLog = (...logLine) => {
  if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE === "1") {
    if (typeof logLine[0] === "string") {
      // allow for printf-like formats to work...
      const [fmt, ...args] = logLine;
      console.log(new Date().toISOString() + " " + fmt, ...args);
    } else {
      // ...but also print objects as first argument as expected
      console.log(new Date().toISOString(), ...logLine);
    }
  }
};

// usable as afterEach hook
async function afterEachCleanup(that, im) {
  const currentTest = that.ctx ? that.ctx.currentTest : that.currentTest;

  im.moveServerLogs(currentTest);

  // .state is "passed" or "failed"
  const testFailed = currentTest.state === "failed";
  const notes = dd`
    Test:
      ${that.title}
    Suite:
      ${that.parent.fullTitle()}
    \n
  `;
  const retainDir = testFailed;
  try {
    await im.cleanup(retainDir, true, notes);
  } catch(e) {
    console.warn(`Ignored cleanup error: ${e}`);
  }
}

exports.sleep = sleep;
exports.debugLog = debugLog;
exports.afterEachCleanup = afterEachCleanup;
