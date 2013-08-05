// Copyright 2013 The Obvious Corporation

var bloom = require('../index'),
  fs = require('fs'),
  assert = require('assert'),
  spawn = require('child_process').spawn,
  sleep = require('sleep').sleep,
  bloomd

/**
 * Delay in ms that we should wait after doing a drop command before issuing
 * a create command to the same filter name, due to bloomd's limitations.
 * If tests are failing, try increasing this.
 */
var DROP_THEN_CREATE_DELAY_MS = 200

/**
 * Delay in seconds that we should wait after starting and stopping the bloomd
 * server to make sure it is ready.
 */
var SERVER_START_STOP_TIME = 1

/**
 * Starts bloomd
 */
function _startServer() {
  bloomd = spawn('bloomd')
  sleep(SERVER_START_STOP_TIME)
}

/**
 * Stops bloomd
 */
function _stopServer() {
  bloomd.kill()
  sleep(SERVER_START_STOP_TIME)
}

/**
 * We use different named filters for each, as bloomd does not allow for the creation
 * of a filter while it is deleting one of the same name, and this library doesn't
 * currently handle this case.
 *
 * These tests work by starting and stopping bloomd a couple of times.
 * It assumes 'bloomd' is an executable on the system.
 *
 * Starting and stopping the server throughout the tests introduces a dependency
 * on the ordering of tests, and makes them potentially non-deterministic. This isn't
 * necessarily best practice but it allows us to test the server going away, and
 * sending commands before the server becomes available.
 */

/**
 * Tests that the client can connect to a bloomd instance which becomes
 * available after the client starts up.
 */
exports.reconnectsOnConnectionFailure = function (test) {
  var filterName = 'reconnects_on_failure'
  var bloomClient = bloom.createClient()

  bloomClient.setSafe(filterName, 'monkey', function(error, data) {
    test.equals(data, true)
  })

  bloomClient.check(filterName, 'monkey', function (error, data) {
    test.equals(data, true)
  })

  bloomClient.drop(filterName, function (error, data) {
    bloomClient.dispose()

    // Set, Create, Set, Check, Drop
    test.equals(5, bloomClient.commandsSent)
    test.done()
  })

  // Start the server only after all the events have been queued.
  _startServer()
}

/**
 * Tests that an unavailable client rejects both queued commands
 * and ones issued after the fact.
 */
exports.unavailableClientRejectsQueuedAndNewCommands = function (test) {
  var filterName = 'unavailable_client_rejection'
  var bloomClient = bloom.createClient({maxConnectionAttempts: 1})

  _stopServer()

  bloomClient.create(filterName, {}, function (error, data) {
    test.equals('Bloomd is unavailable', error.message, 'Command should have been rejected')
  })

  // Wait until we get the unavailable signal, then try a command.
  bloomClient.on('unavailable', function() {
    bloomClient.set(filterName, 'monkey', function (error, data) {
      test.equals('Bloomd is unavailable', error.message, 'Command should have been rejected')
      test.done()
      _startServer()
    })
  })
}

/**
 * Tests that disposing a client doesn't reconnect
 */
exports.disposedClientDoesNotReconnect = function (test) {
  var filterName = 'disposed_client_reconnected'
  var bloomClient = bloom.createClient()

  _stopServer()

  bloomClient.create(filterName, {}, function (error, data) {
    test.equals('Bloomd is unavailable', error.message, 'Command should have been rejected')
    test.done()
    _startServer()
  })

  bloomClient.dispose()
}

// Tests after this point will run with bloomd available.

/**
 * Tests that calling setSafe on a filter actually calls the original
 * callback if the filter already exists.
 */
exports.setAndCreateTestFilterExists = function (test) {
  var filterName = 'set_and_create_already_exists'
  var bloomClient = bloom.createClient()
  var called = false

  // Create a filter.
  bloomClient.create(filterName, {}, function (error, data) {
    test.equals(data, true, 'Failed to create filter')
  })

  bloomClient.setSafe(filterName, 'monkey', function(error, data) {
    test.equals(data, true)
    called = true
  })

  bloomClient.check(filterName, 'monkey', function(error, data) {
    test.equals(data, true)
  })

  bloomClient.drop(filterName, function() {
    bloomClient.dispose()
    test.equals(called, true, 'The original callback was not called')
    test.done()
  })
}

/**
 * Tests the setting of a key on a filter that doesn't exist, that the
 * filter is automatically created, that the key is set, and that the
 * original callback is still called.
 */
exports.setAndCreateTestFilterDoesNotExist = function (test) {
  var filterName = 'set_and_create_non_existent'
  var bloomClient = bloom.createClient()

  bloomClient.drop(filterName, function (error, data) {
    // This is a bit janky, as we have to drop the bloomClient to
    // ensure that the filter doesn't exist beforehand,
    // but bloomd has a period of time where you can't create immediately
    // after a drop where creation will fail, so we wait for a bit.
    // Non-deterministic, but probably ok.
    setTimeout(function() {
      bloomClient.setSafe(filterName, 'monkey', function(error, data) {
        test.equals(data, true)

        // The cleanup drop command also has to come in this callback,
        // otherwise it will be in the queue before the create and retry
        // commands that are generated by the non-existence of the filter.
        // This is why promises are good.
        bloomClient.drop(filterName, function() {
          // Drop, Set, Create, Set, Drop
          bloomClient.dispose()
          test.equals(5, bloomClient.commandsSent)
          test.done()
        })
      })
    }, DROP_THEN_CREATE_DELAY_MS)
  })
}

/**
 * Tests the checking of a key after we call setSafe. When a filter doesn't exist, setSafe
 * automatically creates it, then sets the value.  If a client issues a check command after
 * a setSafe command, it should return true, even if the filter didn't exist.
 */
exports.checkAfterSetSafe = function (test) {
  var filterName = 'check_after_set_safe'
  var bloomClient = bloom.createClient()

  bloomClient.drop(filterName, function (error, data) {
    // Also janky.
    setTimeout(function() {
      bloomClient.setSafe(filterName, 'monkey', function(error, data) {
        test.equals(data, true)
      })

      bloomClient.check(filterName, 'monkey', function(error, data) {
        test.equals(data, true, 'Check after safe set was not true')
      })

      bloomClient.drop(filterName, function() {
        // Drop, Set, Create, Set, Check, Drop
        bloomClient.dispose()
        test.equals(6, bloomClient.commandsSent)
        test.done()
      })
    }, DROP_THEN_CREATE_DELAY_MS)
  })
}

/**
 * Tests interleaved consecutive safe and non-safe commands, to ensure they run in the specified order.
 */
exports.interleavedSafeNonSafe = function (test) {
  var filterName = 'interleaved_safe_non_safe'
  var bloomClient = bloom.createClient()

  bloomClient.drop(filterName, function (error, data) {
    // Also janky.
    setTimeout(function() {
      bloomClient.multiSafe(filterName, ['monkey'], function(error, data) {
        test.deepEqual(data, {
          monkey: false
        })
      })

      bloomClient.bulk(filterName, ['monkey', 'magic', 'muppet'], function(error, data) {
        test.deepEqual(data, {
          monkey: true,
          magic: true,
          muppet: true
        })
      })

      bloomClient.multiSafe(filterName, ['magic', 'muppet', 'moonbeam'], function(error, data) {
        test.deepEqual(data, {
          magic: true,
          muppet: true,
          moonbeam: false
        })
      })

      bloomClient.bulkSafe(filterName, ['monkey', 'moonbeam'], function(error, data) {
        test.deepEqual(data, {
          monkey: false,
          moonbeam: true
        })
      })

      bloomClient.multi(filterName, ['monkey', 'magic', 'muppet', 'moonbeam'], function(error, data) {
        test.deepEqual(data, {
          monkey: true,
          magic: true,
          muppet: true,
          moonbeam: true
        })
      })

      bloomClient.drop(filterName, function() {
        // Drop, Multi, Create, Multi, Bulk, Multi, Bulk, Multi, Drop
        bloomClient.dispose()
        test.equals(9, bloomClient.commandsSent)
        test.done()
      })
    }, DROP_THEN_CREATE_DELAY_MS)
  })
}


/**
 * Tests the setting of a key on a filter that doesn't exist, in the situation
 * where the creation of the filter fails for some reason.
 *
 * We can simulate this by using a sufficiently low desired capacity.
 */
exports.setAndCreateTestFilterCannotBeCreated = function (test) {
  var filterName = 'set_and_create_error_creating'
  var bloomClient = bloom.createClient()

  bloomClient.drop(filterName, function (error, data) {
    // Same as prior test, also janky.
    setTimeout(function() {
      bloomClient.setSafe(filterName, 'monkey', function(error, data) {
        test.equals(error.message, 'Client Error: Bad arguments')

        bloomClient.drop(filterName, function() {
          bloomClient.dispose()
          test.done()
        })
      }, {
        // A low capacity will cause a creation failure due to bad arguments.
        capacity: 100
      })
    }, DROP_THEN_CREATE_DELAY_MS)
  })
}

/**
 * Test insertion and subsequent retrieval of 235k items, into a filter initially
 * sized for 20k, forcing multiple resizes.  We chain the multi on the callback of
 * the bulk in order to get accurate timings.
 */
exports.bulkPerformance = function (test) {
  var filterName = 'bulk_performance'
  var bloomClient = bloom.createClient()

  // Read in a dictionary.
  fs.readFile('./test/words.txt', 'utf8', function (error, data) {

    // Create a filter.
    bloomClient.create(filterName, {
      prob: 0.0001,
      capacity: 20000
    })

	// The last line will be blank.
    var lines = data.split('\n')
    lines.pop()

    var bulkExpected = {}
    var multiExpected = {}

    for (var i = 0, l = lines.length; i < l; i++) {
      var line = lines[i]
      bulkExpected[line] = true
      multiExpected[line] = true
    }

    // There are a couple of collisions at this probability.
    bulkExpected['choledochotomy'] = false
    bulkExpected['ensnarer'] = false
    bulkExpected['renunciatory'] = false
    bulkExpected['unboundless'] = false

    // Insert lots of data.
    var bulkStart = process.hrtime()
    bloomClient.bulk(filterName, lines, function (error, data) {
      var elapsed = bloom.timer(bulkStart, 'Inserted ' + lines.length + ' items')

      // Totally arbitrary, but should be plenty of room on even a moderate laptop.
      test.ok(elapsed < 1000, 'Bulk set considered too slow')

      test.deepEqual(bulkExpected, data)

      var multiStart = process.hrtime()
      bloomClient.multi(filterName, lines, function (error, data) {
        var elapsed = bloom.timer(multiStart, 'Retrieved ' + lines.length + ' items')

        // Totally arbitrary, but should be plenty of room on even a moderate laptop.
        test.ok(elapsed < 1000, 'Multi check considered too slow')
        test.deepEqual(multiExpected, data)
      })

      bloomClient.drop(filterName, function() {
        bloomClient.dispose()
        test.done()
      })
    })
  })
}

/**
 * Test repeated calls to info, to force data buffering that will
 * result in incomplete lists, and give the stream transformation code a workout.
 */
exports.consecutiveInfo = function (test) {
  var filterName = 'consecutive_info'
  var bloomClient = bloom.createClient()

  // Create a filter.
  bloomClient.create(filterName, {
    prob: 0.01,
    capacity: 20000
  })

  var iterations = 1000
  var responseCount = 0
  function callback(error, data) {
    test.equals(data.name, filterName, 'Did not get back the same thing we put it')
    responseCount++
  }

  for (var i = 0; i < iterations; i++) {
    bloomClient.info(filterName, callback)
  }

  bloomClient.drop(filterName, function() {
    bloomClient.dispose()
    test.equals(iterations, responseCount, 'Did not iterate the correct number of times')
    test.done()
  })
}

/**
 * Perform the canonical steps listed in the bloomd readme
 *
 * https://github.com/armon/bloomd/blob/master/README.md
 */
exports.canonicalTest = function (test) {
  var filterName = 'canonical_test'
  var bloomClient = bloom.createClient()

  bloomClient.list(null, function(error, data) {
    test.equals(data.length, 0, 'We have a list, somehow')
  })

  // Create a filter.
  bloomClient.create(filterName, {}, function (error, data) {
    test.equals(data, true, 'Failed to create filter')
  })

  bloomClient.check(filterName, 'zipzab', function (error, data) {
    test.deepEqual(data, false, 'zipzab should not exist')
  })

  bloomClient.set(filterName, 'zipzab', function (error, data) {
    test.equals(data, true, 'zipzab should have been created')
  })

  bloomClient.check(filterName, 'zipzab', function (error, data) {
    test.equals(data, true, 'zipzab should now exist')
  })

  bloomClient.multi(filterName, ['zipzab', 'blah', 'boo'], function (error, data) {
    test.deepEqual(data, {
      zipzab: true,
      blah: false,
      boo: false
    })
  })

  bloomClient.bulk(filterName, ['zipzab', 'blah', 'boo'], function (error, data) {
    test.deepEqual(data, {
      zipzab: false,
      blah: true,
      boo: true
    })
  })

  bloomClient.multi(filterName, ['zipzab', 'blah', 'boo'], function (error, data) {
    test.deepEqual(data, {
      zipzab: true,
      blah: true,
      boo: true
    })
  })

  bloomClient.list(null, function(error, data) {
    test.equals(data.length, 1, 'We had a list, somehow.')
    test.equals(data[0].name, filterName)
  })

  bloomClient.drop(filterName, function (error, data) {
    test.equals(data, true, 'Failed to drop filter')
  })

  bloomClient.list(null, function(error, data) {
    bloomClient.dispose()
    test.equals(data.length, 0, 'We had a list, somehow.')
    test.done()
  })

}

/**
 * Dummy test to kill the server and finish up.
 */
exports.stopServer = function (test) {
  _stopServer()
  test.done()
}
