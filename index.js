var net = require('net'),
    events = require('events')
    util = require('util')
    defaultPort = 8673,
    defaultHost = '127.0.0.1'

/**
 * A client for BloomD
 *
 * TODO(jamie)
 *  + Stream Write Buffering
 *  + Cluster Support
 *  + Connection Pooling
 *  + Retries
 *  + Consistent Hashing By Bloom Filter
 *  + Tests
 *  + Error checking
 *  ? StreamNoDelay configuration
 *
 * Options are:
 *
 * debug: [false] Emit debug information
 *
 * @param stream
 * @param options
 * @returns
 */
function BloomClient(stream, options) {
  this.stream = stream
  this.options = options
  this.connected = false

  var self = this

  this.stream.on('connect', function () {
    self.onConnect()
  })

  this.stream.on('data', function (buffer) {
    self.onData(buffer)
  })

  this.stream.on('error', function(message) {
    self.onError(message.message)
  })

  this.stream.on('close', function() {
    self.connectionGone('close')
  })

  this.stream.on('end', function() {
    self.connectionGone('end')
  })

  events.EventEmitter.call(this)
}
util.inherits(BloomClient, events.EventEmitter)

BloomClient.prototype.onError = function (msg) {
  var message = 'Connection failed to ' + this.host + ':' + this.port + ' (' + msg + ')'
  console.log(message)
  if (this.options.debug) {
    console.warn(message)
  }

  this.connected = false
  this.emit('error', new Error(message))
  this.connectionGone('error')
}

BloomClient.prototype.onConnect = function () {
  console.log("Connected")
  if (this.options.debug) {
    console.log('Connected to ' + this.host + ':' + this.port)
  }

  this.stream.setTimeout(0)
  this.stream.setEncoding('utf8')

  this.emit('connect')
}

BloomClient.prototype.connectionGone = function (reason) {
  if (this.options.debug) {
    console.warn('Connection is gone (' + reason + ')')
  }

  // TODO(jamie) Retries.
}

BloomClient.prototype.onData = function (data) {
  console.log("sdfsdfsf")
  if (this.options.debug) {
    console.log('Got data from ' + this.host + ':' + this.port + ': ' + data.toString())
  }
}

BloomClient.prototype.send = function(command, args, callback) {
  args = args || []
  args.unshift(command)
  console.log(this.stream)
  var line = args.join(' ')

  console.log (line)
  this.stream.write(line)
}


function parseBoolean(data) {
  return data === 'Yes'
}

function parseList(data) {

}

BloomClient.prototype.create = function (filterName, options) {
  var args = []
  for (var key in options) {
    args.push(key + '=' + options[key])
  }
  this.send('create', args, parseBoolean)
}

BloomClient.prototype.list = function (prefix) {
  this.send('list', null, parseList)
}

BloomClient.prototype.drop = function (filterName) {

}

BloomClient.prototype.close = function (filterName) {

}

BloomClient.prototype.clear = function (filterName) {

}

BloomClient.prototype.check = function (filterName, key) {

}

BloomClient.prototype.multi = function (filterName, keys) {

}

BloomClient.prototype.set = function (filterName, key) {

}

BloomClient.prototype.bulk = function (filterName, keys) {

}

BloomClient.prototype.info = function (filterName) {

}

BloomClient.prototype.flush = function (filterName) {

}

exports.BloomClient = BloomClient

exports.createClient = function (options) {
  host = options.host || defaulHost
  port = options.port || defaultPort

  var netClient = net.createConnection(port, host)
  var bloomClient = new BloomClient(netClient, options)

  return bloomClient
}

var bloomClient = exports.createClient({
  host: "127.0.0.1",
//  host: "10.249.70.166",
  debug: true
})
bloomClient.list()