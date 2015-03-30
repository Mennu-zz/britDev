var Q = require('q')
var requestInterval = 10000 // 5 secs

var collector = Collector.pull({
  flexible: false, // excdepr for scenario every field can be changed with subsqequent queries, defaults to false
  scenario: 'us-airports', // scenario
  path: 'http://services.faa.gov/airport/status/LAX', //path
  method: 'get', // request method, default is get
  query: 'format=application/json', // any query params
  body: ''
})

// if flexible == false: params query and body can be changed with subsequent queries, others cannot

setInterval(function () {
  //console.log('Collector making request')
  collector.request() // making the request here
}, requestInterval)

// acceptData function is mandatory, has access to the res object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {

  console.log('LAX COLLECTOR DATA')
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
      updateTime: Date.now(),
      scenario: 'us-airports-lax',
      body: message
    })

  })

  return d.promise
}

