var Q = require('q')
var requestInterval = 5000 // 10 secs

var collector = Collector.pull({
  flexible: false, // excdepr for scenario every field can be changed with subsqequent queries, defaults to false
  scenario: 'us-airports', // scenario
  path: 'http://services.faa.gov/airport/status/SFO', //path
  method: 'get', // request method, default is get
  query: 'format=application/json', // any query params
  body: ''
})

// listen to "refreshRequest" for real-time queries
collector.on('refreshRequest', function (params) { // collectorName is mandarory for refreshers

  // if required, check for conditions in params - for eg: caching
  //console.log('refreshRequest CALLED', params)
  // console.log('ADDRESS:', collectorName)
  collector.request() // {} and collectorName mandarory for refreshers
})

// acceptData function is mandatory, has access to the res object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {

  console.log('SFO COLLECTOR REFRESH DATA')

  console.log(message)
  console.log('--------------------------')
  console.log(message.origin) // WILL NOT BE AUTO-CONVERTED, STRINGIFY YOUR SELF!
  console.log('--------------------------')
  console.log(message.body)
  console.log('--------------------------')
  console.log(message.time)
  console.log('\n**************************\n')

  var d = Q.defer()

  process.nextTick(function () {

    d.resolve({
      id: Date.now(),
      body: message.body
    })

  })

  return d.promise
}

