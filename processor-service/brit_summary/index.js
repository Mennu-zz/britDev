var Q = require('q'),count=0,cacheIds,users;
var databaseUrl = "mongodb://0.0.0.0:27017/cw-api1";
var Db = require('mongodb');


var loki = require('lokijs'),db1 = new loki('/home/naveen/heirarchy.json');
var heirarchy;
var processor = Processor.subscribe({
  channels: ['processor:britannia_try','collector:pull:query_master']// can listen to multiple channels
})

function processSummaryAndSaveViews(){
	var d = Q.defer();
	if(cacheIds && users){
		ci = cacheIds;
		Db.MongoClient.connect(databaseUrl,{auto_reconnect: true }, function(err, db) {
			if(err) throw err;
			var views = db.collection("views");
			var batch = db.collection("views").initializeUnorderedBulkOp();
			//for(i=0;i<ci.length;ci++){
			while(ci.length>0){
				var vid = ci.splice(0,1);
		        (function(vid){
		        	user = users[vid];
		          	views.findOne({_id:vid},function(err,view){
		            	if(users[vid]){
				            view.processedData.username = user["staff-name"]
				            view.processedData.email = user["staff-email"]
				            view.processedData.level = user.level
				            view.processedData.code = user.code
				            view.processedData.primarySales = Math.ceil(Number(user.primarySales)/1000)
				            view.processedData.isSalesManOnGprs = user.is_sm_on_gprs
				            view.processedData.firstHalfAttended = user.first_half_attend
				            view.processedData.firstHalfOrders = user.first_half_orders
				            view.processedData.secondHalfAttended = user.sec_half_attend
				            view.processedData.secondHalfOrders = user.sec_half_orders
				            view.processedData.isProcessed = false
				            view.processedData.key = user.level + ":" + user.code
				            view.processedData.parentCode = user["parent-code"]
				            view.processedData.parentLevel = user["parent-level"]
				            view.processedData.name = user["name"]
				                if(user.level == 'distributor'){
				                  view.processedData.awLastSyncDatetime = user["AW_last_sync_datetime"]
				                }
				          }
		          		reportees = view.processedData.body[2].content;
				          views.find({_id:{$in:view.reporteeQuery}},{_id:1,"tmpSummary":1}).toArray(function(err,rv){
				          	for(ri=0;ri<rv.length;ri++){
				            //reportee's level and code
				            tmpcache = rv[ri];
				            //tmpcache = cache[reportee[ri]["vpath"]];
				            //tmpcache = heirarchy.findOne({vid:vid});
				            sales = [user["name"]||""];
				                dist = [user["name"]||""];
				                edge = [user["name"]||""];
				                sales.push(tmpcache.tmpSummary.sales.BCR);
				                sales.push(tmpcache.tmpSummary.sales.Dairy);
				                dist.push(tmpcache.tmpSummary.dist.ECO);
				                dist.push(tmpcache.tmpSummary.dist["New Outlets"]);
				                edge.push(tmpcache.tmpSummary.edge.TLSD);
				                view.processedData.body[1].content[0].data.push(sales);
				                view.processedData.body[1].content[1].data.push(dist);
				                view.processedData.body[1].content[2].data.push(edge);
				          		if(ri+1 == rv.length){
				          			batch.insert(view);
				          			if(ci.length==0){
				          				batch.execute(function(err,result){
							              if(err){
							                console.log("Error");
							                console.log(err);
							              }
							              d.resolve();
							            });			
				          			}
				          		}
				          }
				          console.log(vid+" Created");
				          });
		            });
		        })(vid);
			}
		});
	}
	return d.promise;
}

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {
//console.log(message);
  var d = Q.defer()
  if(message.body && message.body.final){
  	//console.log(message.body);
  	count++;
  	if(message.body.note=="cache"){
  		console.log("Got Cache Data");
      //heirarchy = db1.loadCollection( {name:"children"} )
  		cacheIds = message.body.cache//,users
  	}else{
  		console.log("Got Users data");
  		users = message.body.users
  	}
  }
  //console.log(count);
  if(count > 0 && count%2 == 0){
  	//start generating summary
  	console.log("Starting the Summary");
  	count=0;

  	processSummaryAndSaveViews().then(function(){
  		console.log("Views processed and saved. Done for the day.");
  		d.resolve();
  	})

  	//As this is the final call prepare the final status and send a mail ..zi!!!
  }

  /*process.nextTick(function () {

    console.log('--- STORED SFO DATA ---')
    d.resolve(message.body) // resolve, else others will not be able to listen to it

  })*/

  return d.promise
}
