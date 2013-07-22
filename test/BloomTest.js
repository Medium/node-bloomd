var bloom = require('../index'),
	fs = require('fs'),
	assert = require('assert')

/**
 * Helper function to time performance in ms.
 *
 * @param {Array} since A previous call to process.hrtime()
 * @param {string} message an optional message
 * @return {number}
 */
function elapsedTime(since, message) {
	var interval = process.hrtime(since)	
	var elapsed = (interval[0] * 1000) + (interval[1] / 1000000)
	message = message ? message + ': ' : ''
	console.log(message + elapsed.toFixed(3) + "ms")
	return elapsed
}

/**
 * We use different named filters for each, as bloomd does not allow for the creation
 * of a filter while it is deleting one of the same name, and this library doesn't
 * currently handle this case.
 *
 * TODO(jamie) Find a way to generalise the boilerplate.
 */

/** 
 * Test insertion of 235k items, into a filter initially sized for 20k, forcing multiple resizes.
 */
exports.bulkPerformance = function (test) {
	var filterName = 'bulk_performance'
	var bloomClient = bloom.createClient()

	bloomClient.on('ready', function() {				
		// Read in a dictionary.
		fs.readFile('./test/words.txt', 'utf8', function (error, data) {

			// Create a filter.
			bloomClient.create(filterName, {
				prob: 0.01,
				capacity: 20000
			})

			// Insert lots of data.
			var lines = data.split('\n')
			var start = process.hrtime()
			bloomClient.bulk(filterName, lines, function (error, data) {
				var elapsed = elapsedTime(start, "Inserted " + lines.length + " items")
				
				// Totally arbitrary, but should be plenty of room on even 
				// a moderate laptop with a single worker.
				test.ok(elapsed < 1000, "Bulk insert considered too slow")
			})
			
			bloomClient.drop(filterName, function() {
				bloomClient.dispose()
				test.done()				
			})			
		});
	})
}

/** 
 * Test repeated calls to info, to force data buffering that will
 * result in incomplete lists, and give the stream transformation code a workout.
 */
exports.consecutiveInfo = function (test) {
	var filterName = 'consecutive_info'
	var bloomClient = bloom.createClient()

	bloomClient.on('ready', function() {				
		// Create a filter.
		bloomClient.create(filterName, {
			prob: 0.01,
			capacity: 20000
		})

		var iterations = 1000
		var responseCount = 0		
		function callback(error, data) {
			test.equals(data.name, filterName, "Didn't get back the same thing we put it")
			responseCount++		
		}

		for (var i = 0; i < iterations; i++) {
			bloomClient.info(filterName, callback)
		}
		
		bloomClient.drop(filterName, function() {
			bloomClient.dispose()
			test.equals(iterations, responseCount, "Didn't iterate the correct number of times")
			test.done()
		})
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
	
	bloomClient.on('ready', function() {
		// Create a filter.
		bloomClient.list(null, function(error, data) {
			test.equals(data.length, 0, "We had a list, somehow.")
		})
		
		bloomClient.create(filterName, {}, function (error, data) {
			test.equals(data, true, "Failed to create filter")
		})

		bloomClient.check(filterName, 'zipzab', function (error, data) {
			test.deepEqual(data, false, "zipzab should not exist")
		})

		bloomClient.set(filterName, 'zipzab', function (error, data) {
			test.equals(data, true, "zipzab should have been created")
		})
		
		bloomClient.check(filterName, 'zipzab', function (error, data) {
			test.equals(data, true, "zipzab should now exist")
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
			test.equals(data.length, 1, "We had a list, somehow.")
			test.equals(data[0].name, filterName)
		})
		
		bloomClient.drop(filterName, function (error, data) {
			test.equals(data, true, "Failed to drop filter")
		})
		
		bloomClient.list(null, function(error, data) {
			bloomClient.dispose()
			test.equals(data.length, 0, "We had a list, somehow.")
			test.done()			
		})
	})	

}