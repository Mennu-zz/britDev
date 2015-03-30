var Q = require('q')

var processor = Processor.subscribe({
  channels: ['processor:sap-po', 'processor:us-airports-lax', 'processor:us-airports-sfo']// listen to multiple channels
})

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

  // IF ALL GOOD

  process.nextTick(function () {

    console.log('--- CONSOLIDATOR ---')
    d.resolve({
      processor: 'CONSOLIDATOR',
      content: message.body
    })

  })

  return d.promise
}
