var bloom = require('../index')

exports.testBloom = function (test) {
  var bloomClient = bloom.createClient({
    host: "10.249.70.166",
    debug: true
  })

  var result = bloomClient.create('monkey_magic', {
    prob: 0.01,
    capacity: 20000
  })
}
