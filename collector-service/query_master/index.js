var Q = require('q'),async = require("async"),users={};
var databaseUrl = "mongodb://0.0.0.0:27017/cw-api";
var Db = require('mongodb'),startedAt,
tempLevel = {
  "region":"national",
  "area":"region",
  "som":"area",
  "territory":"som",
  "distributor":"territory",
  "salesman":"distributor",
  "outlet":"salesman"
},   MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server;
//var db = new Db('cw-api', new Server('localhost', 27017),{safe: false, strict: false});
var requestInterval = 5000 // 10 secs
//var fetchUrl = "http://115.249.190.247:8080/uCLMS/salesData?APIKey=hgd832384234&backlogin=1&";
//http://125.16.214.232:8080/uCLMS/salesData?APIKey=hgd832384234&backlogin=1&level=region&code=SCH2

    var collector = Collector.pull({
    flexible: true, // excdepr for scenario every field can be changed with subsqequent queries, defaults to false
    scenario: 'britannia-teams', // scenario
    path: "http://125.16.214.232:8080/uCLMS/salesData", //path
    method: 'get', // request method, default is get
    query: 'APIKey=hgd832384234&backlogin=1&query=masters', // any query params
    body: '',
    timeout:120
  })

  // listen to "refreshRequest" for real-time queries
  collector.on('refreshRequest', function (params) { // collectorName is mandarory for refreshers
    // if required, check for conditions in params - for eg: caching
    console.log('refreshRequest CALLED', params)
     startedAt = new Date(parseInt(params.replace("startedAt=","")))
    collector.request() // {} and collectorName mandarory for refreshers
  })


  //Extract level and code from data
  /*Just saving the hierarchy users list and sending the each hierarchy 
  object to the processor for further processing.*/

  function extractData(data){
    var d = Q.defer();
    console.log(data.length);
//    Db.MongoClient.connect(databaseUrl, function(err, db) {
  //    if(err) throw err;

      //TODO
    //  var batch = db.collection("views").initializeUnorderedBulkOp(),keys = {};
      while(data.length > 0){
        var user = data.shift();
        if(user.salesUser == "true"){
          var tmpqm = user;
          tmpqm._id = user.level+":"+user.code
          users[tmpqm._id] = tmpqm;
        }
        if(data.length==0){
          //batch.execute(function(err,result){
                //console.log(result);
            //    db.close();
                d.resolve({
                  users : users
                });
          //});
        }
      }
    //});
    return d.promise;
  }
  // acceptData function is mandatory, has access to the res object for extra details
  // it MUST return a promise, use whatever
  function acceptData(err, message,headers) {
    var d = Q.defer()
    if(err){
      console.log("Error Occured");
      console.log(err.code);
      if(err.code=="ETIMEDOUT"){
      	console.log(headers);
      }
      d.reject({error : err})
    }
    console.log('SFO COLLECTOR REFRESH DATA');
    //sample level will be replaced through the query parameters
    console.log("Data recieved at "+Date.now())
    console.log("Time Taken "+(Date.now()-startedAt.getTime())/60000+" mins");
    //console.log(message.body)
    //console.log(message.body["data"])

    var data = JSON.parse(message.body)["data"];

    extractData(data).then(function(records){
      console.log("Summary Processor getting invoked");
      //console.log(records);
      Db.MongoClient.connect(databaseUrl, {
            auto_reconnect: true
        }, function(err, db) {

        	db.collection("cpStatus").insert({
        					title:"Collector started data extraction.",
        					from:"extractionCollector",
        					startedAt:startedAt,
        					endTime:new Date()
        				},function(err,res){
						    d.resolve({
						        id: Date.now(),
						        users: users,
						        final:true
						      });
        				});
        });
    });

    return d.promise;
  }

  //db.close();
