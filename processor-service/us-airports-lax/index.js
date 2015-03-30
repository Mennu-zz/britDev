var Q = require('q')

var processor = Processor.subscribe({
  channels: ['collector:pull:us-airport-lax']// can listen to multiple channels
})

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {

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

    console.log('--- STORED LAX DATA ---')
    console.log(message)
    d.resolve({
      airport: 'LAX',
      parinam: message.body // WILL NOT BE AUTO-CONVERTED, STRINGIFY YOUR SELF!
    }) // resolve, else others will not be able to listen to it

  })

  return d.promise
}
