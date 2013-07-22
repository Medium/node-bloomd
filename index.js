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
 *  + Stream Write Buffering
 *  + Command Buffering for quick calls after client creation.
 *  + Partial block rememberance, to avoid rework.
 *  + Retry and reconnect support
 *  + More Error checking
 *  ? StreamNoDelay configuration
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

	var self = this

	stream.on('connect', function() {
		self.onConnect()
	})
	
	stream.on('error', function(message) {
		self.onError(message.message)
	})
  
	stream.on('close', function() {
		self.connectionClosed('close')
	})

	stream.on('end', function() {
		self.connectionClosed('end')
	})

	this.stream.pipe(this.responseParser)
	
	this.responseParser.on('readable', function() {
		self.onReadable()
	})
	
	events.EventEmitter.call(this)
}
util.inherits(BloomClient, events.EventEmitter)

/**
 * Fires when the parser is able to send back a complete response from the server.
 *
 * Because operations are performed in the order they are received, we can safely
 * unshift a command off the queue and use it to match the response to the callback
 * that is waiting for it.
 */
BloomClient.prototype.onReadable = function () {
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
BloomClient.prototype.onConnect = function () {
	if (this.options.debug) {
		console.log('Connected to ' + this.options.host + ':' + this.options.port)
	}

  	this.ready = true
  	this.emit('ready')
}

/**
 * Fires when there is an error on the underlying stream.
 */
BloomClient.prototype.onError = function (msg) {
  var message = 'Connection failed to ' + this.options.host + ':' + this.options.port + ' (' + msg + ')'
  if (this.options.debug) {
    console.warn(message)
  }

  this.connected = false
  this.emit('error', new Error(message))
  this.connectionClosed('error')
}

/**
 * Fires when a connection is closed, either through error, or naturally.
 *
 * TODO(jamie) Support reconnects, and flushing of queue for natural closure.
 *
 * @param {string} reason
 */
BloomClient.prototype.connectionClosed = function (reason) {
  if (this.options.debug) {
    console.warn('Connection closed (' + reason + ')')
  }
}

/**
 * Sends a command to the server.
 * 
 * @param {string} command
 * @param {Array} args
 * @param {string} responseType one of ResponseParser.responseTypes
 * @param {Function} callback
 */
BloomClient.prototype.send = function(command, args, responseType, callback) {
	args = args || []
	args.unshift(command)
	var line = args.join(' ') + '\n'

	this.commandQueue.push({
		arguments: args,
		responseType: responseType,
		callback: callback
	})
	
	this.stream.write(line)
}

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

// Commands

/**
 * Creates a named bloom filter.
 *
 * A number of options are available.  Stated defaults are for those of 
 * a bloomd server with default configuration. Check your server for
 * specifics.
 *
 * prob      [0.001]  - The desired probability of false positives.
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
  this.send('create', args, responseTypes.CONFIRMATION, callback)
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
	this.send('list', args, responseTypes.FILTER_LIST, callback)
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
	this.send('drop', [filterName], responseTypes.CONFIRMATION, callback)
}

/**
 * Closes a filter.
 * 
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.close = function (filterName, callback) {
	this.send('close', [filterName], responseTypes.CONFIRMATION, callback)
}

/**
 * Clears a filter.
 * 
 * @param {string} filterName
 * @param {Function} callback
 */
BloomClient.prototype.clear = function (filterName, callback) {
	this.send('clear', [filterName], responseTypes.CONFIRMATION, callback)
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
	this.send('check', [filterName, key], responseTypes.BOOL, callback)
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
	this.send('multi', keys, responseTypes.BOOL_LIST, callback)
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
	this.send('set', [filterName, key], responseTypes.BOOL, callback)
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
	this.send('bulk', keys, responseTypes.BOOL_LIST, callback)
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
	this.send('info', [filterName], responseTypes.INFO, callback)
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
	this.send('flush', args, responseTypes.CONFIRMATION, callback)
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

