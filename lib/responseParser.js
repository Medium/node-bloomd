// Copyright 2013 The Obvious Corporation

var stream = require('stream'),
    util = require('util')

/**
 * A named bloom filter object.
 */
function BloomFilter() {
  this.capacity = null
  this.checks = null
  this.checkHits = null
  this.checkMisses = null
  this.name = null
  this.pageIns = null
  this.pageOuts = null
  this.probability = null
  this.sets = null
  this.setHits = null
  this.setMisses = null
  this.size = null
  this.storage = null
}

/**
 * A parser for data returned by bloomd
 *
 * Provides a stream transformer to extract responses from
 * the server, and static methods to parse them into native
 * JS types.
 */
function ResponseParser(client) {
  this.client = client
  this.lines = []
  this.lineData = ''
  this.blockLines = 1
  stream.Transform.call(this, {objectMode: true})
}
util.inherits(ResponseParser, stream.Transform);

/**
 * An enumeration of possible response types.
 */
ResponseParser.responseTypes = {
  BOOL: 'bool',
  BOOL_LIST: 'boolList',
  CONFIRMATION: 'confirmation',
  DROP_CONFIRMATION: 'dropConfirmation',
  FILTER_LIST: 'filterList',
  INFO: 'info'
}

/**
 * Given a chunk of data, appends onto previously received data,
 * decomposes it into lines and parses those lines into either single
 * or multi-line responses that can be used to populate JS types.
 *
 * @param {Buffer} chunk
 * @param {string} encoding
 * @param {Function} done
 */
ResponseParser.prototype._transform = function (chunk, encoding, done) {

  // Add the chunk to the line buffer
  this.lineData += chunk.toString()
  var lines = this.lineData.split(/\r\n|\r|\n/g);

  // If the chunk finishes on a newline, the final line
  // will be empty, otherwise it will be a partially completed
  // line.  Either way, we don't want to process it.
  this.lineData = lines.pop()

  for (var i = 0, l = lines.length; i < l; i++) {
    this.lines.push(lines[i])
  }

  parseLines:
  while (this.lines.length) {

    if ('START' === this.lines[0]) {
      for (var i = this.blockLines, l = this.lines.length; i < l; i++) {
        if ('END' === this.lines[i]) {
          // Got a full list. Push it and continue parsing.
          this.push(this.lines.splice(0, i + 1).slice(1, -1))

          // Reset the block count for the next block.
          this.blockLines = 1

          // Goto might be considered harmful, but this is a continue ;)
          continue parseLines
        }
      }

      // We had an incomplete list, so we have to wait until we get more data.
      // Remember which line we got to, so we don't have to start from 1 again.
      this.blockLines = i
      break
    } else {
      this.push(this.lines.shift())
    }
  }

  done()
}

// Static Converters

/**
 * Parses a Yes/No response from bloomd into a boolean.
 *
 * @param {string} data
 * @return {bool}
 */
ResponseParser.parseBool = function (data) {
  if ('Yes' === data) {
    return true
  } else if ('No' === data) {
    return false
  } else {
    throw new Error(data)
  }
}

/**
 * Pairs a list of Yes/No responses with the queried keys and
 * returns a map between the two.
 *
 * @param {string} data
 * @param {Array} keys
 * @return {bool}
 */
ResponseParser.parseBoolList = function (data, keys) {
  var values = data.split(' '),
    results = {}

  try {
    for (var i = 0, l = values.length; i < l; i++) {
      results[keys[i]] = ResponseParser.parseBool(values[i])
    }
  } catch (err) {
    // If there was an error parsing a bool, make the entire line available for debugging.
    throw new Error(data)
  }

  return results
}

/**
 * Parses a Done response from bloomd into a boolean.
 *
 * @param {string} data
 * @return {bool}
 */
ResponseParser.parseConfirmation = function (data) {
  if ('Done' === data) {
    return true
  } else {
    throw new Error(data)
  }
}

/**
 * Parses a Done response from bloomd into a boolean, following a drop command.
 *
 * For drop commands, we don't care if the filter existed or not.
 *
 * @param {string} data
 * @return {bool}
 */
ResponseParser.parseDropConfirmation = function (data) {
  if ('Done' === data || 'Filter does not exist' === data) {
    return true
  } else {
    throw new Error(data)
  }
}

/**
 * Parses a list of filter definitions into an array of BloomFilter objects.
 *
 * @param {Array} data
 * @return {Array}
 */
ResponseParser.parseFilterList = function (data) {
  if (!Array.isArray(data)) {
    throw new Error(data)
  }
  return data.map(function(item) {
    var definition = item.split(' ')
    var filter = new BloomFilter()
    filter.name = definition[0]
    filter.probability = definition[1]
    filter.storage = definition[2]
    filter.capacity = definition[3]
    filter.size = definition[4]
    return filter
  })
}

/**
 * Parses filter information into a single BloomFilter objects.
 *
 * @param {Array} data
 * @return {BloomFilter}
 */
ResponseParser.parseInfo = function (data, name) {
  if (!Array.isArray(data)) {
    throw new Error(data)
  }
  var filter = new BloomFilter()
  for (var i = 0, l = data.length; i < l; i++) {
    var definition = data[i].split(' ')
    filter[definition[0].replace(/_([a-z])/g, function (g) { return g[1].toUpperCase() })] = definition[1]
  }
  filter.name = name
  return filter
}

// Exports

exports.ResponseParser = ResponseParser
