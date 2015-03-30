
var collector = Collector.pull({
  scenario: 'discodance',
  endpoint: 'http://www.britindia.com/api', // remote endpoint
  method: 'post', // defaults to post
  query: 'n=YA&a=89', // this can be over-written anytime with a new value while making a request
  body: '808098', // this can be over-written anytime with a new value while making a request
  headers: [ // this can be over-written anytime with a new value while making a request
    "'x-access-code': '89056'",
    "'x-sign': '235'"
  ]
})

function validateData(data) {
  // data.body, data.workitemType, data.lastSyncTime, data.collector, data.raw: ?
  if (data.body.length > 0) {
    return true // valid data, return true to pass
    // lastSyncTime will be updated by the framework, if returned true
  }
  // not returning anything means, it is invalid
}

function acceptData(data) {

  // data.body, data.workitemType, data.lastSyncTime
  if (data.lastSyncTime !== this.lastSyncTime) { // data sync has happened successfully

    // store the data using whatever data connectors - should return an object with data id
    // built-in data connector: cw-module-collector-datastore

    // data store event will be automatically published -> collector:britannia-heirarchy:view - data

  }

}


// example of a periodic Pull - every 10 mins
var periodicPull = setInterval(function () {

  collector.request({ // check cache before requesting again
    query: 'n=LA&a=578',
    body: 'POPMAN',
    headers: [
      "'x-access-code': '89056'",
      "'x-sign': '235'"
    ]
  }).then(validateData).then(storeData, function (fail) {
    // do something about this
  })

}, 600000)


// example of a scheduled pull - at midnight
var scheduledPull = setInterval(function () {

  var d = new Date()
  if (d.getHours == 0) {

    collector.request({ // check cache before requesting again
      query: 'n=LA&a=578',
      body: 'POPMAN',
      headers: [
        "'x-access-code': '89056'",
        "'x-sign': '235'"
      ]
    }).then(validateData).then(storeData, function (fail) {
      // do something about this
    })

  }
}, 1000) // check every sec for the time


// example of a realtime pull - listen to the refreshRequest event
collector.on('refreshRequest', function (params) {

  var query = 'n=' + params.n + '&' + 'a=' + params.a
  var body = params.body

  collector.request({
    query: query,
    body: body
  }, validateData, storeData)

})

