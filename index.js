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
 *  + Safe Dropping
 *  + Retry and reconnect support
 *  + Maintain order of commands on a filter, even when using *safe.
 *  + More Error checking
 *  ? StreamNoDelay configuration
 *  ? Rename setSafe command to set, and set to setUnchecked
 *
 * Options are:
 *
 * debug: [false] Emit debug information
 *
 * @param {Object} stream
 * @param {Object} options
 */
function BloomClient(stream, options) {
  this.stream = stream
  this.options = options
  this.responseParser = new ResponseParser(this)
  this.ready = false
  this.commandQueue = []
  this.offlineQueue = []
  this.commandsSent = 0

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
 * Whether or not the client can accept commands.
 *
 * @return {bool}
 */
BloomClient.prototype.isReady = function() {
  return this.ready
}

/**
 * Closes the connection to BloomD
 */
BloomClient.prototype.dispose = function () {
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
 * prob      [0.0001]  - The desired probability of false positives.
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
  var args = [filterName]
  options = options || {}
  for (var key in options) {
    args.push(key + '=' + options[key])
  }
  this._process('create', args, responseTypes.CONFIRMATION, callback)
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
  this._process('list', args, responseTypes.FILTER_LIST, callback)
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
  this._process('drop', [filterName], responseTypes.CONFIRMATION, callback)
}

/**
 * Closes a filter.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.close = function (filterName, callback) {
  this._process('close', [filterName], responseTypes.CONFIRMATION, callback)
}

/**
 * Clears a filter.
 *
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.clear = function (filterName, callback) {
  this._process('clear', [filterName], responseTypes.CONFIRMATION, callback)
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
  this._process('c', [filterName, key], responseTypes.BOOL, callback)
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
  keys.unshift(filterName)
  this._process('m', keys, responseTypes.BOOL_LIST, callback)
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
  this._process('s', [filterName, key], responseTypes.BOOL, callback)
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
  keys.unshift(filterName)
  this._process('b', keys, responseTypes.BOOL_LIST, callback)
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
  this._process('info', [filterName], responseTypes.INFO, callback)
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
  this._process('flush', args, responseTypes.CONFIRMATION, callback)
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

/**
 * Alias for multi, for ease of remembering.
 *
 * Multi checks many items.
 *
 * @see BloomClient.prototype.multi
 */
BloomClient.prototype.multiCheck = BloomClient.prototype.multi

/**
 * Safe versions of standard functions.
 * They appear on the prototype as setSafe, checkSafe, bulkSafe etc.
 *
 * @see _makeSafe()
 */
var _safeCommands = ['set', 'check', 'bulk', 'multi', 'info']
for (var i = 0, l = _safeCommands.length; i < l; i++) {
  var command = _safeCommands[i]
  BloomClient.prototype[command + 'Safe'] = _makeSafe(BloomClient.prototype[command])
}

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

        case responseTypes.INFO:
          // command.arguments[1] is the name of the filter, passed back for completeness.
          data = ResponseParser.parseInfo(response, command.arguments[1])
          break

        default:
          throw new Error('Unknown response type: ' + command.responseType)
          break
      }
    } catch (err) {
      error = err
    }

    // Callbacks are optional.
    if (command.callback) {
      command.callback(error, data)
    }
  }
}

/**
 * Fires when the underlying stream connects.
 *
 * TODO(jamie) Support queuing of commands before ready, and then flush them here.
 */
BloomClient.prototype._onConnect = function () {
  if (this.options.debug) {
    console.log('Connected to ' + this.options.host + ':' + this.options.port)
  }

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
  this.emit('error', new Error(message))
  this._connectionClosed('error')
}

/**
 * Fires when a connection is closed, either through error, or naturally.
 *
 * TODO(jamie) Support reconnects, and flushing of queue for natural closure.
 *
 * @param {string} reason
 */
BloomClient.prototype._connectionClosed = function (reason) {
  if (this.options.debug) {
    console.warn('Connection closed (' + reason + ')')
  }
}

/**
 * Prepares a command to be sent.  If the stream is ready to receive a command, 
 * sends it immediately, otherwise queues it up to be sent when the stream is ready.
 *
 * @param {string} commandName
 * @param {Array} args
 * @param {string} responseType one of ResponseParser.responseTypes
 * @param {Function} callback
 */
BloomClient.prototype._process = function(commandName, args, responseType, callback) {
  args = args || []
  args.unshift(commandName)
  var command = {
    arguments: args,
    responseType: responseType,
    callback: callback
  }
  
  if (this.ready) {
    if (this.options.debug) {
      console.log("Processing:", commandName)
    }
    this._send(command)
  } else {
    if (this.options.debug) {
      console.log("Buffering command:", commandName)
    }
    this.offlineQueue.push(command)
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
    console.log("Sent:", command.arguments[0])
  }

  this.commandsSent++
  this.commandQueue.push(command)

  if (!processedEntirely) {
    if (this.options.debug) {
      console.log("Waiting after full buffer:", command.arguments[0])
    }
    this.ready = false
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
      console.log("Sending buffered command:", command.arguments[0])
    }
    
    if (!this._send(command)) {
      // Buffer was filled from this command.  Wait some more.
      return
    }
  }
  this.ready = true
  this.emit('ready')
}

// Helper Functions

/**
 * Returns a function which is a 'safe' version of the supplied command. That is, if
 * the filter doesn't exist when the command is run, attempts to automatically create
 * the filter and then re-run the command, transparently to the client.
 *
 * If there is an error in the creation step, this function returns filter creation
 * failure, not the original 'filter not found', to help track down why the creation
 * would be failing.
 *
 * @param {Function} command
 * @return {Function}
 */
function _makeSafe(command) {
  return function() {
    // This is a function like setSafe()
    var self = this
    var args = Array.prototype.slice.call(arguments, 0)
    var filterName = args[0]
    var createOptions = {}
    var callback
    if (args[args.length - 1] instanceof Function) {
      callback = args.pop()
    } else {
      createOptions = args.pop()
      callback = args.pop()
    }
    args.push(function(originalError, originalData) {
      // This is the callback which catches the response to the original command
      // (e.g. safe, check, bulk, multi etc.)
      if (originalError && ('Filter does not exist' === originalError.message)) {
        // Filter didn't exist, try and create it.
        self.create(filterName, createOptions, function (createError, createData) {
          // This is callback which catches the response to the create command.
          if (createError) {
            // Creation of the filter failed. Run the original callback with this failure.
            callback.call(callback, createError, createData)
          } else {
            // Creation succeeded, re-run the command.
            command.apply(self, args)
          }
        })
      } else {
        // The filter exists, so run the original callback.
        callback.call(callback, originalError, originalData)
      }
    })
    // Execute the requested command.
    command.apply(self, args)
  }
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

exports.print = function (error, data) {
    console.log(data)
}

