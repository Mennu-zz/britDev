var Q = require('q'),nodemailer = require('nodemailer'),db,
    processCount = 0,startTime,count=0,
    cacheIds, users, views, batch = null, async = require('async');
var databaseUrl = "mongodb://0.0.0.0:27017/cw-api";
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
		        //console.log("Got the View "+view._id);
		        //console.log("Step 1 :"+view._id);
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
		        //console.log("Step 2 :"+view._id);
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
				//		     	console.log("Step 4 :"+view._id);
		                    	console.log(view._id + " saving to batch execution. :"+processCount);
				                batch.insert(view);
				                if (processCount > 1599) {
				                    processCount=0;
				                    batch.execute(function(err, res) {
				                        if (err) {throw err;}
				                        batch = null;
				                        batch = _db.collection("tmpViews").initializeUnorderedBulkOp();
				                        callback();
				                    });
				                } else {
				                    callback();
				                }
						    }else{
						    	tmpcache = item;
						    	console.log(item);
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
		        } else {
		        //	console.log("Step 3.1 :"+view._id);
		        	console.log(view._id + " saving to batch execution. :"+processCount);
		            batch.insert(view);
		            if (processCount > 1599) {
		            	processCount = 0;
		                batch.execute(function(err, res) {
		                    if (err) {console.log(err);}
		                    batch = null;
		                    batch = _db.collection("tmpViews").initializeUnorderedBulkOp();
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

function prepareAndSendMail(){
	var d = Q.defer();
	var transporter = nodemailer.createTransport({
	    service: 'Gmail',
	    auth: {
	        user: 'naveenmeherch@gmail.com',
	        pass: 'mennunav'
	    }
	});
	_db.collection("cpStatus").findOne({from:"extractionCollector"},function(err,doc){
	
		var table = "<b>Please find the Brit Exporter status below.</b><br><br><table><tr><td >Tasks</td><td>Start Time</td><td>End Time</td><td>Time Taken</td></tr>";
		table+='<tr><td >Collected Data from the Api and partial processed.</td><td>'+new Date(doc.startedAt)+'</td><td>'+new Date(startTime)+'</td><td>'+(doc.endTime-startTime)/1000+' mins </td></tr>';
		table+='<tr><td >Renderiing Views Completed.</td><td>'+new Date(startTime)+'</td><td>'+new Date()+'</td><td>'+(Date.now()-startTime)/1000+' mins </td></tr>';
		table+='</table>';
		// setup e-mail data with unicode symbols
		var mailOptions = {
		    from: 'Test Space', // sender address
		    to: 'naveen.meher@incture.com', // list of receivers
		    subject: '✔ Brit Test Run Status', // Subject line
		    html:  table// html body
		};
	//startTime
		// send mail with defined transport object
		transporter.sendMail(mailOptions, function(error, info){
		    if(error){
		        console.log(error);
		        d.resolve();
		    }else{
		        console.log('Message sent: ' + info.response);
		        d.resolve();
		    }
		});

	})
	
	return d.promise;
}

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message) {
    //console.log(message);
    var d = Q.defer()
    if (message.body && message.body.final) {
        //console.log(message.body);
       // count++;
        if (message.body.note == "cache") {
            console.log("Got Final Signal from Cache Data");
            //heirarchy = db1.loa dCollection( {name:"children"} )
            //cacheIds = message.body.cache //,users
        } else {
            console.log("Got Users data");
            users = message.body.users
	console.log(users)
        }
    }else{
    	d.resolve();
    }
    //console.log(count);
    if (count == 2) {
        //start generating summary
        console.log("Starting the Summary @ "+Date.now());
        startTime = Date.now();
        count = 0;
        Db.MongoClient.connect(databaseUrl, {
            auto_reconnect: true
        }, function(err, db) {
        	_db = db;
        	
			batch = db.collection("tmpViews").initializeUnorderedBulkOp();
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
                        	prepareAndSendMail().then(function(){
                        		db.close();
		                        d.resolve({msg:"Done"});
                        	});
                        });
                });
            });
        });
        //As this is the final call prepare the final status and send a mail ..zi!!!
    }else{
    	d.resolve();
    }
    return d.promise;
}
