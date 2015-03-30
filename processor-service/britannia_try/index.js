var Q = require('q');
var databaseUrl = "mongodb://0.0.0.0:27017/cw-api",processor;
var Db = require('mongodb'),db;
Db.MongoClient.connect(databaseUrl, function(err, dbConn) {
	db = dbConn;
	processor = Processor.subscribe({
	  channels: ['collector:pull:sampleTry']// listen to multiple channels
	})
});

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {

  // console.log('acceptData:')
  // console.log(req)

  var d = Q.defer()

  process.nextTick(function () {

    console.log('--- GOT DATA ---')
    //console.log(message.body)
    d.resolve('SUCCESS!')

  })

  return d.promise
}
