
var Q = require('q')

var collector = Collector.push({
  scenario: 'po',
  method: 'post' // defaults to post for push controllers
})

// function is optional, but MUST return true to validate, if defined
function accessControl(req) {
  if (req.query.x === 'x') {
    return true
  }
}

// function is optional, MUST return true to pass, if defined
function dataValidation(req) {
  if (req.body) {
    return true
  }
}

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(req) {

  // console.log('dataStore:')
  // console.log(req)

  var d = Q.defer()

  process.nextTick(function () {

    console.log('STORING DATA FROM PUSH COLLECTOR: %s', req.query.name)

    d.resolve({
      id: Date.now(),
      collector: 'sap-po'
    })

  })

  return d.promise
}

