var Q = require('q')

var processor = Processor.subscribe({
  channels: ['collector:push:sap-po']// listen to multiple channels
})

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {

  // console.log('acceptData:')
  // console.log(req)

  var d = Q.defer()

  process.nextTick(function () {

    console.log('--- GOT DATA ---')
    console.log(message.body)
    d.resolve('SUCCESS!')

  })

  return d.promise
}
