var Q = require('q'),
    processCount = 0,count=0,
    cacheIds, users, views, batch = null, async = require('async');
var databaseUrl = "mongodb://0.0.0.0:27017/cw-api1";
var Db = require('mongodb');
var heirarchy;
var processor = Processor.subscribe({
    channels: ['processor:britannia_try', 'collector:pull:query_master'] // can listen to multiple channels
})



function processSummaryAndSaveViews(vid, callback) {
    (function(vid){
    	user = users[vid._id] || {};
	    views.findOne({
	        _id: vid._id
	    }, function(err, v) {
	    	(function(v){
	    		view = v;
		        processCount++;
		        console.log("Got the View "+view._id);
		        console.log("Step 1 :"+view._id);
		        if (user) {
		            view.processedData.username = user["staff-name"]
		            view.processedData.email = user["staff-email"]
		            view.processedData.level = user.level
		            view.processedData.code = user.code
		            view.processedData.primarySales = Math.ceil(Number(user.primarySales) / 1000)
		            view.processedData.isSalesManOnGprs = user.is_sm_on_gprs
		            view.processedData.firstHalfAttended = user.first_half_attend
		            view.processedData.firstHalfOrders = user.first_half_orders
		            view.processedData.secondHalfAttended = user.sec_half_attend
		            view.processedData.secondHalfOrders = user.sec_half_orders
		            view.processedData.isProcessed = false
		            //view.processedData.key = user.level + ":" + user.code
		            view.processedData.parentCode = user["parent-code"]
		            view.processedData.parentLevel = user["parent-level"]
		            view.processedData.name = user["name"]
		            if (user.level == 'distributor') {
		                view.processedData.awLastSyncDatetime = user["AW_last_sync_datetime"]
		            }
		            view.reporteeNames = "removed";
		        }
		        console.log("Step 2 :"+view._id);
		        if (view.reporteeQuery && view.reporteeQuery.length > 0) {
		            views.find({
		                _id: {
		                    $in: view.reporteeQuery
		                }
		            }, {
		                _id: 1,
		                "tmpSummary": 1
		            },function(err,resCursor){
		            	function processReporteeItem(err,item){
		            		if(item === null) {
						      // All done!
						     	console.log("Step 4 :"+view._id);
		                    	console.log(view._id + " saving to batch execution. :"+processCount);
				                batch.insert(view);
				                if (processCount > 3000) {
				                    processCount=0;
				                    batch.execute(function(err, res) {
				                        if (err) {throw err;}
				                        return callback();
				                    });
				                } else {
				                    return callback();
				                }
						    }else{
						    	tmpcache = item;
			            		sales = [user["name"] || view.reporteeNames && view.reporteeNames[tmpcache._id] || ""];
			                    dist = [user["name"] || view.reporteeNames &&  view.reporteeNames[tmpcache._id] || ""];
			                    edge = [user["name"] || view.reporteeNames &&  view.reporteeNames[tmpcache._id] || ""];
			                    sales.push(tmpcache.tmpSummary.sales.BCR);
			                    sales.push(tmpcache.tmpSummary.sales.Dairy);
			                    dist.push(tmpcache.tmpSummary.dist.ECO);
			                    dist.push(tmpcache.tmpSummary.dist["New Outlets"]);
			                    edge.push(tmpcache.tmpSummary.edge.TLSD);
			                    view.processedData.body[1].content[0].data.push(sales);
			                    view.processedData.body[1].content[1].data.push(dist);
			                    view.processedData.body[1].content[2].data.push(edge);
			                    resCursor.nextObject(processReporteeItem);
						    }
		            	}
		            	resCursor.nextObject(processReporteeItem);
		            });
		            /*views.find({
		                _id: {
		                    $in: view.reporteeQuery
		                }
		            }, {
		                _id: 1,
		                "tmpSummary": 1
		            }).toArray(function(err, rv) {
		            	console.log(rv);
		            	console.log("Step 3 :"+view._id);
		            	while(rv.length>0){
		            		tmpcache = rv.shift();
		            		sales = [user["name"] || view.reporteeNames && view.reporteeNames[tmpcache._id] || ""];
		                    dist = [user["name"] || view.reporteeNames &&  view.reporteeNames[tmpcache._id] || ""];
		                    edge = [user["name"] || view.reporteeNames &&  view.reporteeNames[tmpcache._id] || ""];
		                    sales.push(tmpcache.tmpSummary.sales.BCR);
		                    sales.push(tmpcache.tmpSummary.sales.Dairy);
		                    dist.push(tmpcache.tmpSummary.dist.ECO);
		                    dist.push(tmpcache.tmpSummary.dist["New Outlets"]);
		                    edge.push(tmpcache.tmpSummary.edge.TLSD);
		                    view.processedData.body[1].content[0].data.push(sales);
		                    view.processedData.body[1].content[1].data.push(dist);
		                    view.processedData.body[1].content[2].data.push(edge);
		                	
		                    if(rv.length==0){
		                    	console.log("Step 4 :"+view._id);
		                    	console.log(view._id + " saving to batch execution. :"+processCount);
				                batch.insert(view);
				                if (processCount > 25000) {
				                    processCount=0;
				                    batch.execute(function(err, res) {
				                        if (err) {throw err;}
				                        cb();
				                    });
				                } else {
				                    cb();
				                }	
		                    }
		            	}
		            });*/
		        } else {
		        	console.log("Step 3.1 :"+view._id);
		        	console.log(view._id + " saving to batch execution. :"+processCount);
		            batch.insert(view);
		            if (processCount > 3000) {
		            	processCount = 0;
		                batch.execute(function(err, res) {
		                    if (err) {console.log(err);}
		                    return callback();
		                })
		            } else {
		                return callback();
		            }
		        }
		    })(v);
	    });
    })(vid);
}


// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {
    //console.log(message);
    var d = Q.defer()
    if (message.body && message.body.final) {
        //console.log(message.body);
        count++;
        if (message.body.note == "cache") {
            console.log("Got Final Signal from Cache Data");
            //heirarchy = db1.loa dCollection( {name:"children"} )
            //cacheIds = message.body.cache //,users
        } else {
            console.log("Got Users data");
            users = message.body.users
        }
    }
    //console.log(count);
    if (count == 1) {
        //start generating summary
        console.log("Starting the Summary @ "+Date.now());
        count = 0;
        Db.MongoClient.connect(databaseUrl, {
            auto_reconnect: true
        }, function(err, db) {
        	batch = db.collection("finalViews").initializeUnorderedBulkOp();
            db.collection('views').find({}, {
                _id: 1
            }).toArray(function(err, data) {
                cacheIds = data;
                console.log("Got the ids, total : " + cacheIds.length);
                views = db.collection("views");
                async.eachSeries(cacheIds, processSummaryAndSaveViews, function(err) {
                    console.log("Saving Views");
                    batch.execute(function(err, results) {
                        console.log("Views Generated @"+Date.now());
                        d.resolve({msg:"Done"});
                        db.close();
                    });
                });
            });
        });
        //As this is the final call prepare the final status and send a mail ..zi!!!
    }
    return d.promise
}
