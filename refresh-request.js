var cs = require('cw-service-collector')

cs.refresh('collector:pull:sampleTry', 'name=YAhoo&age=23', function () {
  console.log('Refresh requested')
})

cs.refresh('collector:pull:query_master', 'startedAt='+Date.now(), function () {
  console.log('Refresh requested')
})

cs.listen('processor:sampleTry', function (err, data) {

  if (!err) {
    console.log('GOT RESPONSE from PROCESSOR')
    console.log(data)
  }

})
