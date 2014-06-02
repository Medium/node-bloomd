// Copyright 2013 The Obvious Corporation

var net = require('net'),
    events = require('events'),
    stream = require('stream'),
    ResponseParser = require('./lib/responseParser').ResponseParser,
    responseTypes = ResponseParser.responseTypes
    util = require('util'),
    defaultPort = 8673,
    defaultHost = '127.0.0.1'

/**
 * A client for BloomD (https://github.com/armon/bloomd)
 *
 * Requires Node 0.10's stream transformations.
 *
 * Opens a single stream and continually writes data to it, offloading the
 * resulting data to a parser and then applying a queued callback to the response.
 * This relies on the fact that queries to bloomd are answered in the
 * order that they were made.
 *
 * The documentation consistently states that checks will return true if the key
 * is in the filter.  Of course, this is only 'true' in the context of bloom filters.
 * False positives are possible.  False negatives are not.
 *
 * More info here: http://en.wikipedia.org/wiki/Bloom_filter
 *
 * TODO(jamie)
 *  + Handle list with prefix after safe commands.
 *  + Safe creation after dropping
 *  + Mock service for better testing.
 *  ? StreamNoDelay configuration
 *
 * Options are:
 *
 * debug                 [false] Emit debug information
 * maxConnectionAttempts [0]     The number of times to try reconnecting. 0 for infinite.
 * reconnectDelay        [160]   The additional time in ms between each reconnect retry.
 * maxErrors             [0]     The number of internal errors received from bloomd after which time
 *                                 the service is marked as unavailable. 0 for infinite.
 *
 * @param {Object} stream
 * @param {Object} options
 */
function BloomClient(stream, options) {
  this.options = options
  this.responseParser = new ResponseParser(this)

  // Connection handling
  this.disposed = false
  this.stream = stream
  this.connectionAttempts = 1
  this.maxConnectionAttempts = options.maxConnectionAttempts || 0
  this.reconnectDelay = options.reconnectDelay || 160
  this.reconnector = null

  // Queue handling
  this.unavailable = false
  this.buffering = true
  this.commandQueue = []
  this.offlineQueue = []
  this.commandsSent = 0
  this.filterQueues = {}

  // Error handling
  this.maxErrors = options.maxErrors || 0
  this.errors = 0

  var self = this

  stream.on('connect', function() {
    self._onConnect()
  })

  stream.on('error', function(message) {
    self._onError(message.message)
  })

  stream.on('close', function() {
    self._connectionClosed('close')
  })

  stream.on('end', function() {
    self._connectionClosed('end')
  })

  stream.on('drain', function () {
    self._drain()
  })

  this.stream.pipe(this.responseParser)

  this.responseParser.on('readable', function() {
    self._onReadable()
  })

  events.EventEmitter.call(this)

}
util.inherits(BloomClient, events.EventEmitter)

// API

/**
 * Whether or not the client will process commands immediately.
 *
 * @return {bool}
 */
BloomClient.prototype.isBuffering = function() {
  return this.buffering
}

/**
 * Allows client code to request a reconnection.
 *
 * node-bloomd automatically attempts to connect or reconnect to the bloomd
 * server. However, if a connectTimeout option is specified, or if the
 * maximum number of internal errors is reached, node-bloomd eventually
 * gives up. This method allows long running processes to request
 * a reconnection in that eventuality.
 *
 * This command is ignored unless the client is marked unavailable, because if
 * it is not marked unavailable, we are still going through the standard retry
 * progression.
 *
 */
BloomClient.prototype.reconnect = function() {
  if (!this.unavailable) {
    return
  }

  // Reset the critical data.
  this.unavailable = false
  this.totalReconnectionTime = 0
  this.connectionAttempts = 0
  this.errors = 0
  this._reconnect()
}

/**
 * Closes the connection to BloomD
 */
BloomClient.prototype.dispose = function () {
  this.disposed = true
  this.stream.end()
}

// Standard Bloomd Commands

/**
 * Creates a named bloom filter.
 *
 * A number of options are available.  Stated defaults are for those of
 * a bloomd server with default configuration. Check your server for
 * specifics.
 *
 * prob      [0.0001] - The desired probability of false positives.
 * capacity  [100000] - The required initial capacity of the filter.
 * in_memory [0]      - Whether the filter should exist only in memory, with no disk backing.
 *
 * The data passed back to the callback will be true on success, null otherwise.
 *
 * @param {string} filterName
 * @param {Object} options
 * @param {Function} callback
 */
BloomClient.prototype.create = function (filterName, options, callback) {
  var self = this
  var args = [filterName]
  options = options || {}
  for (var key in options) {
    args.push(key + '=' + options[key])
  }
  this._process('create', filterName, args, responseTypes.CREATE_CONFIRMATION, function (error, data) {

    // First, run the callback.
    if (callback) {
      callback.call(callback, error, data)
    }

    // Then, clear the filter queue if we have one.
    self._clearFilterQueue(filterName)
  })
}

/**
 * Lists filters matching the specified optional prefix.
 *
 * If no prefix is specified, all filters are returned.
 *
 * The data passed back to the callback will be an array of BloomFilter objects.
 *
 * @param {string} prefix
 * @param {Function} callback
 */
BloomClient.prototype.list = function (prefix, callback) {
  var args = prefix ? [prefix] : []
  this._process('list', null, args, responseTypes.FILTER_LIST, callback)
}

/**
 * Drops the specified filter.
 *
 * The data passed back to the callback will be true if the filter was dropped successfully.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.drop = function (filterName, callback) {
  this._process('drop', filterName, [filterName], responseTypes.DROP_CONFIRMATION, callback)
}

/**
 * Closes a filter.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.close = function (filterName, callback) {
  this._process('close', filterName, [filterName], responseTypes.CONFIRMATION, callback)
}

/**
 * Clears a filter.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.clear = function (filterName, callback) {
  this._process('clear', filterName, [filterName], responseTypes.CONFIRMATION, callback)
}

/**
 * Checks to see if a key is set in the filter.
 *
 * The data passed back to the callback will be true if it is
 * in the filter or false, if it is not.
 *
 * @param {string} filterName
 * @param {string} key
 * @param {Function} callback
 */
BloomClient.prototype.check = function (filterName, key, callback) {
  this._handle(this._buildCheckCommand(filterName, key, callback))
}

/**
 * Checks to see if multiple keys are set in the filter.
 *
 * The data passed back to the callback will be an object map with
 * key mapped to a boolean value indicating its presence in the filter.
 *
 * @param {string} filterName
 * @param {Array} keys
 * @param {Function} callback
 */
BloomClient.prototype.multi = function (filterName, keys, callback) {
  this._handle(this._buildMultiCommand(filterName, keys, callback))
}

/**
 * Sets a key in the filter.
 *
 * The data passed back to the callback will be true if the key was newly set,
 * or false if it was already in the filter.  The latter is not considered an error.
 *
 * @param {string} filterName
 * @param {string} key
 * @param {Function} callback
 */
BloomClient.prototype.set = function (filterName, key, callback) {
  this._handle(this._buildSetCommand(filterName, key, callback))
}

/**
 * Sets multiple keys in the filter.
 *
 * The data passed back to the callback will be an object map with
 * key mapped to a boolean value; true if the key was newly
 * set, false if it was already in the set.
 *
 * @param {string} filterName
 * @param {Array} keys
 * @param {Function} callback
 */
BloomClient.prototype.bulk = function (filterName, keys, callback) {
  this._handle(this._buildBulkCommand(filterName, keys, callback))
}

/**
 * Retrieves information about the specified filter.
 *
 * The data passed back to the callback will be a single BloomFilter object.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.info = function (filterName, callback) {
  this._process('info', filterName, [filterName], responseTypes.INFO, callback)
}

/**
 * Flushes filters to disk.
 *
 * If a filter name is provided, that filter is flushed, otherwise
 * all filters are flushed.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.flush = function (filterName, callback) {
  var args = filterName ? [filterName] : []
  this._process('flush', filterName, args, responseTypes.CONFIRMATION, callback)
}

// 'Safe' Commands

BloomClient.prototype._buildCheckCommand = function (filterName, key, callback) {
  return this._buildCommand('check', filterName, [filterName, key], responseTypes.BOOL, callback)
}

BloomClient.prototype._buildMultiCommand = function (filterName, keys, callback) {
  var args = keys.slice(0)
  args.unshift(filterName)
  return this._buildCommand('multi', filterName, args, responseTypes.BOOL_LIST, callback)
}

BloomClient.prototype._buildSetCommand = function (filterName, key, callback) {
  return this._buildCommand('set', filterName, [filterName, key], responseTypes.BOOL, callback)
}

BloomClient.prototype._buildBulkCommand = function (filterName, keys, callback) {
  var args = keys.slice(0)
  args.unshift(filterName)
  return this._buildCommand('bulk', filterName, args, responseTypes.BOOL_LIST, callback)
}


/**
 * Safe versions of standard functions.
 * They appear on the prototype as setSafe, checkSafe, bulkSafe etc.
 *
 * @see _makeSafe()
 */
var _safeCommands = ['set', 'check', 'bulk', 'multi']
for (var i = 0, l = _safeCommands.length; i < l; i++) {
  var commandName = _safeCommands[i]
  BloomClient.prototype[commandName + 'Safe'] = _makeSafe(commandName)
}

// Extended Commands

/**
 * Alias for bulk, for ease of remembering.
 *
 * Bulk sets many items.
 *
 * @see BloomClient.prototype.bulk
 */
BloomClient.prototype.bulkSet = BloomClient.prototype.bulk
BloomClient.prototype.bulkSetSafe = BloomClient.prototype.bulkSafe

/**
 * Alias for multi, for ease of remembering.
 *
 * Multi checks many items.
 *
 * @see BloomClient.prototype.multi
 */
BloomClient.prototype.multiCheck = BloomClient.prototype.multi
BloomClient.prototype.multiCheckSafe = BloomClient.prototype.multiSafe

// Private Methods

/**
 * Fires when the parser is able to send back a complete response from the server.
 *
 * Because operations are performed in the order they are received, we can safely
 * unshift a command off the queue and use it to match the response to the callback
 * that is waiting for it.
 */
BloomClient.prototype._onReadable = function () {
  var response
  while (response = this.responseParser.read()) {
    var command = this.commandQueue.shift(),
        error = null,
        data = null

    if (this.options.debug) {
      _timer(command.started, 'Response received for: ' + command.filterName + ' ' + command.arguments[0])
    }

    if (ResponseParser.isError(response)) {
      this.errors++
      if (this.maxErrors && (this.errors >= this.maxErrors)) {
        return this._unavailable()
      }
      error = new Error('Bloomd Internal Error')
    } else {
      if (this.errors > 0) {
        this.errors--
      }
      try {
        switch (command.responseType) {
          case responseTypes.BOOL:
            data = ResponseParser.parseBool(response)
            break

          case responseTypes.BOOL_LIST:
            data = ResponseParser.parseBoolList(response, command.arguments.slice(2))
            break

          case responseTypes.FILTER_LIST:
            data = ResponseParser.parseFilterList(response)
            break

          case responseTypes.CONFIRMATION:
            data = ResponseParser.parseConfirmation(response)
            break

          case responseTypes.CREATE_CONFIRMATION:
            data = ResponseParser.parseCreateConfirmation(response)
            break

         case responseTypes.DROP_CONFIRMATION:
            data = ResponseParser.parseDropConfirmation(response)
            break

          case responseTypes.INFO:
            data = ResponseParser.parseInfo(response, command.filterName)
            break

          default:
            throw new Error('Unknown response type: ' + command.responseType)
            break
        }
      } catch (err) {
        error = command.error || err
      }
    }

    // Callbacks are optional.
    if (command.callback) {
      error.command = command.arguments
      command.callback(error, data)
    }
  }
}

/**
 * Fires when the underlying stream connects.
 */
BloomClient.prototype._onConnect = function () {
  if (this.options.debug) {
    console.log('Connected to ' + this.options.host + ':' + this.options.port)
  }

  this.unavailable = false

  this.emit('connected')
  this._drain()
}

/**
 * Fires when there is an error on the underlying stream.
 */
BloomClient.prototype._onError = function (msg) {
  var message = 'Connection failed to ' + this.options.host + ':' + this.options.port + ' (' + msg + ')'
  if (this.options.debug) {
    console.warn(message)
  }

  this.connected = false
  this._connectionClosed('error')
}

/**
 * Fires when a connection is closed, either through error, or naturally.
 *
 * @param {string} reason
 */
BloomClient.prototype._connectionClosed = function (reason) {
  if (this.options.debug) {
    console.warn('Connection closed (' + reason + ')')
  }
  this.buffering = true

  this.emit('disconnected')
  this._reconnect()
}

/**
 * Attempts to reconnect to the underlying stream.
 */
BloomClient.prototype._reconnect = function () {
  if (this.reconnector || this.unavailable) {
    // We explicitly disposed the client, or we've exhausted our
    // attempts to connect, so no need to reconnect.
    return
  }

  var self = this

  if (this.disposed || (this.maxConnectionAttempts && (this.connectionAttempts >= this.maxConnectionAttempts))) {
    // We've hits the max number of connection attempts, or we have been disposed.
    // Mark the client as unavailable, which will also reject the various queues.
    if (this.options.debug) {
      console.log('Bloomd is unavailable.')
    }
    this._unavailable()
    return
  }

  // Simple linear back-off. Defaults would give ms delays of [160, 320, 480, ...]
  var reconnectDelay = this.connectionAttempts * this.reconnectDelay

  this.connectionAttempts++
  this.reconnector = setTimeout(function () {
    if (self.options.debug) {
      console.log('Connecting: attempt (' + self.connectionAttempts + ')')
    }
    if (!self.disposed) {
      self.stream.connect(self.options.port, self.options.host)
    }
    self.reconnector = null
  }, reconnectDelay)
  this.reconnector.unref()
}

/**
 * Marks the client as unavailable.
 *
 * An unavailable client will fail all commands sent to it immediately, as well
 * as fail all commands that have been requested but not responded to.
 */
BloomClient.prototype._unavailable = function() {
  var command
  this.unavailable = true

  // Clear the command queue.
  while (command = this.commandQueue.shift()) {
    this._rejectCommand(command)
  }

  while (command = this.offlineQueue.shift()) {
    this._rejectCommand(command)
  }

  for (var filterName in this.filterQueues) {
    var queue = this.filterQueues[filterName]
    while (command = queue.shift()) {
      this._rejectCommand(command)
    }
  }

  this.filterQueues = {}

  // Announce that we are unavailable.
  this.emit('unavailable')
}

/**
 * Rejects a command that cannot be processed.
 *
 * If a callback was specified, it will be called with an error.
 *
 * @param {Object} command
 */
BloomClient.prototype._rejectCommand = function (command) {
  if (this.options.debug) {
    console.log('Rejecting command:', command.arguments[0], command.filterName)
  }
  if (command.callback) {
    command.callback(new Error('Bloomd is unavailable'), null)
  }
}

/**
 * Convenience function to build and handle a command.
 *
 * @param {string} commandName
 * @param {string} filterName
 * @param {Array} args
 * @param {string} responseType one of ResponseParser.responseTypes
 * @param {Function} callback
 */
BloomClient.prototype._process = function (commandName, filterName, args, responseType, callback) {
  this._handle(this._buildCommand(commandName, filterName, args, responseType, callback))
}

/**
 * Prepares a command from the supplied arguments.
 *
 * @param {string} commandName
 * @param {string} filterName
 * @param {Array} args
 * @param {string} responseType one of ResponseParser.responseTypes
 * @param {Function} callback
 */
BloomClient.prototype._buildCommand = function (commandName, filterName, args, responseType, callback) {
  args = args || []
  args.unshift(commandName)
  return {
    filterName: filterName,
    arguments: args,
    responseType: responseType,
    callback: callback
  }
}

/**
 * Prepares a command to be sent.  If the stream is ready to receive a command,
 * sends it immediately, otherwise queues it up to be sent when the stream is ready.
 *
 * @param {Object} command
 * @param {boolean} clearing
 */
BloomClient.prototype._handle = function (command, clearing) {
  var commandName = command.arguments[0]
  var filterName = command.filterName

  if (this.unavailable) {
    this._rejectCommand(command)
    return
  }

  if (filterName && this.filterQueues[filterName] && ('create' !== commandName) && !clearing) {
    // There are other commands outstanding for this filter, so hold this one until they are processed.
    if (this.options.debug) {
      console.log('Holding command in filter sub-queue:', commandName, filterName)
    }
    this.filterQueues[filterName].push(command)
    return
  }

  if (this.buffering) {
    if (this.options.debug) {
      console.log('Buffering command:', commandName)
    }
    this.offlineQueue.push(command)
  } else {
    if (this.options.debug) {
      console.log('Processing:', commandName)
    }
    this._send(command)
  }
}

/**
 * Attempts to send a command to bloomd.  If the command was sent, pushes it
 * onto the command queue for processing when the response arrives.
 *
 * Returns a boolean indicating sent status.
 *
 * @param {Object} command
 * @return {boolean}
 */
BloomClient.prototype._send = function (command) {
  var line = command.arguments.join(' ') + '\n'
  var processedEntirely = this.stream.write(line)

  if (this.options.debug) {
    console.log('Sent:', command.arguments[0])
    command.started = process.hrtime()
  }

  this.commandsSent++
  this.commandQueue.push(command)

  if (!processedEntirely) {
    if (this.options.debug) {
      console.log('Waiting after full buffer:', command.arguments[0])
    }
    this.buffering = true
  }

  return processedEntirely
}

/**
 * Processes the offline command queue.
 *
 * Marks the client as ready when there is nothing left in the queue.
 */
BloomClient.prototype._drain = function () {
  while (this.offlineQueue.length) {
    var command = this.offlineQueue.shift()
    if (this.options.debug) {
      console.log('Sending buffered command:', command.arguments[0])
    }

    if (!this._send(command)) {
      // Buffer was filled from this command.  Wait some more.
      return
    }
  }
  this.buffering = false
  this.emit('drain')
}

/**
 * Queues for processing all those commands which were held due to
 * a 'safe' method being invoked.
 *
 * @param {string} filterName
 */
BloomClient.prototype._clearFilterQueue = function (filterName) {
  var filterQueue = this.filterQueues[filterName]
  if (!filterQueue) {
    return
  }

  if (this.options.debug) {
    console.log('Clearing filter queue:', filterName)
  }

  while (filterQueue.length) {
    this._handle(filterQueue.shift(), true)
  }

  delete this.filterQueues[filterName]
}

// Helper Functions

/**
 * Returns a function which is a 'safe' version of the command with the supplied name. That is, if
 * the filter doesn't exist when the command is run, attempts to automatically create
 * the filter and then re-run the command, transparently to the client.
 *
 * If there is an error in the creation step, the callback will receive the filter creation
 * failure, not the original 'filter not found', to help track down why the creation
 * would be failing.
 *
 * @param {string} command
 * @return {Function}
 */
function _makeSafe(commandName) {
  var commandBuilder = BloomClient.prototype['_build' + commandName[0].toUpperCase()  + commandName.slice(1) + 'Command']

  return function() {
    // This is a function like setSafe()
    var self = this
    var args = Array.prototype.slice.call(arguments, 0)
    var filterName = args[0]
    var createOptions = {}

    // Supports optional createOptions as a final parameter.
    var callback
    if (args[args.length - 1] instanceof Function) {
      callback = args.pop()
    } else {
      createOptions = args.pop()
      callback = args.pop()
    }

    // Create a separate copy of these arguments, so they don't get munged by later commands
    // which modify them.
    var originalArgs = args.slice(0)
    originalArgs.push(callback)

    args.push(function (originalError, originalData) {
      // This is the callback which catches the response to the original command
      // (e.g. safe, check, bulk, multi etc.)
      if (originalError && ('Filter does not exist' === originalError.message)) {
        // Try to create the filter.  The create method will clear the queue when it completes.
        self.create(filterName, createOptions, function (createError, createData) {
          // This is the callback which catches the response to the create command.
          // In it, we tell it to run the command which triggered this creation.
          var command = commandBuilder.apply(self, originalArgs)

          // If the creation fails, the triggering action will also fail.
          // Store the creation error so we can give useful feedback for why the triggering
          // action wasn't successful, despite it being 'safe'.
          if (createError) {
            command.error = createError
          }

          self._handle(command, true)
        })
      } else {
        // The filter exists, so run the original callback.
        callback.call(callback, originalError, originalData)

        self._clearFilterQueue(filterName)
      }
    })

    this._handle(commandBuilder.apply(self, args))

    // Create a queue for this filter, so that subsequent commands to this filter are
    // buffered until it is created.
    if (!this.filterQueues[filterName]) {
      this.filterQueues[filterName] = []
    }
  }
}

/**
 * Helper function to time performance in ms.
 *
 * @param {Array} since A previous call to process.hrtime()
 * @param {string} message an optional message
 * @return {number}
 */
function _timer(since, message) {
  var interval = process.hrtime(since)
  var elapsed = (interval[0] * 1000) + (interval[1] / 1000000)
  message = message ? message + ': ' : ''
  console.log(message + elapsed.toFixed(3) + 'ms')
  return elapsed
}

// Exports

exports.BloomClient = BloomClient

exports.createClient = function (options) {
  options = options || {}
  options.host = options.host || defaultHost
  options.port = options.port || defaultPort

  var netClient = net.createConnection(options.port, options.host)
  return new BloomClient(netClient, options)
}

exports.timer = _timer
