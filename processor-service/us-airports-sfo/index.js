var Q = require('q')

var processor = Processor.subscribe({
  channels: ['collector:pull:us-airport-sfo']// can listen to multiple channels
})

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {

  console.log('--- SFO PROCESSOR REFRESH DATA ---')
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

    console.log('--- STORED SFO DATA ---')
    d.resolve(message.body) // resolve, else others will not be able to listen to it

  })

  return d.promise
}
