var Q = require('q'),async = require('async'),britCache = [];//britCache = {};
var databaseUrl = "mongodb://0.0.0.0:27017/cw-api";
var Db = require('mongodb'),loki = require('lokijs'),db1 = new loki('/home/naveen/heirarchy.json'),
dateSuffix = [ "null","th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th","th", "th", "th", "th", "th", "th", "th", "th", "th", "th","th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th","th", "st" ];

var heirarchy = db1.addCollection('children',["vid"]);
//collector:pull:sampleTry
var processor = Processor.subscribe({
  channels: ['collector:pull:sampleTry']// can listen to multiple channels
})

function processTargetVsAchievement(data,cb){
  var d = Q.defer();
  var summaryTmp = {sales:{},dist:{},edge:{}};
  var series = {
    sales:["BCR","Dairy"],
    dist:["ECO","New Outlets"],
    edge:["TLSD"]
  }
  var tables = {
              type: 'tables',
              title: 'Target vs Achievement',
              subTitle: 'Values in Thousand(s)',
              name: 'targetdata',
              content: []
            };
  var tableContents = {
      sales: {
        type: 'table',
        title: 'Sales (000)',
        name: 'salesdata',
        data: [
          ['CAT', 'LM', 'TGT', 'MTD', '(%)','DRR']
        ]
      },
      dist: {
        type: 'table',
        title: 'Dist',
        name: 'distdata',
        data: [
          ['CAT', 'LM', 'TGT', 'MTD', '(%)','DRR']
        ]
      },
      edge: {
        type: 'table',
        title: 'EDGE',
        name: 'edgedata',
        data: [
          ['CAT', 'LM', 'TGT', 'MTD', '(%)','DRR']
        ]
      }
    };
    var targets = data.targetdata;
    if(targets){
      var cols = Object.keys(series);
      //si -> series Index
      for(si=0;si<cols.length;si++){
        //soi -> series object index
        for(soi=0;soi<series[cols[si]].length;soi++){
            var obj = targets[series[cols[si]][soi]];
            if(obj){
            	var lm = obj.lm && Math.round(obj.lm/1000)||0,
	            tgt = obj.tgt && Math.round(obj.tgt/1000)||0,
	            mtd = obj.mtd && Math.round(obj.mtd/1000)||0,
	            rr = obj.rr && Math.round(obj.rr/1000)||0,
	            percentage = (tgt != 0 && mtd != 0 && (mtd/tgt)*100)|| 0;
	            //console.log(message);

	            tableContents[cols[si]].data.push([obj.displayName, lm, tgt, mtd, percentage.toFixed(1), rr]);
	            summaryTmp[cols[si]][obj.displayName] = mtd +"/"+ tgt+" ("+Math.round((mtd/tgt)*100)+" %)";
            }else{
            	tableContents[cols[si]].data.push([series[cols[si]][soi], 0, 0, 0, 0, 0]);
	        	summaryTmp[cols[si]][series[cols[si]][soi]] = 0 +"/"+ 0+" ("+0+" %)";
            }
        }
      }
      tables.content.push(tableContents);
      //console.log(summaryTmp);
      d.resolve({table : tables,summaryTmp : summaryTmp});
    }else{
      d.resolve(null,null);
    }
    return d.promise;
}

function processTrends(data){
  var d = Q.defer();

    var monthDisplay = ["null",
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December',
    ]

    var chartObjects = []
    var dailyCharts = []
    var trends = data.trenddata;
    if(trends){
    	var trendKeys = Object.keys(trends);
    	for(i=0;i<trendKeys.length;i++){

    		var chartObject = {}, dailyChart = {};
    		chartObject.title = trends[trendKeys[i]].displayName;
	        dailyChart.title = trends[trendKeys[i]].displayName;
	        chartObject.highChartsData = {
	          title: { text: chartObject.title,
	                   style: {display: 'None'} },
	          chart: { type: 'column' },
	          xAxis: { categories: [] },
	          series: [{ data: []}],
	          legend: { enabled: false }
	        };
	        dailyChart.highChartsData = {
	          title: { text: trends[trendKeys[i]].displayName,
	                   style: {display: 'None'} },
	          chart: { type: 'column' },
	          xAxis: { categories: [] },
	          series: [{ data: []}],
	          legend: { enabled: false }
	        };
	        var months = Object.keys(trends[trendKeys[i]].monthly);
	        var dates = Object.keys(trends[trendKeys[i]].daily);
	        //console.log(trends[trendKeys[i]]);
	        //adds date to the given date sting and sorts them.
	        months.sort(function(a,b){
				var c = new Date( "01/"+a.replace( /(\d{2})\/(\d{2})\/(\d{4})/, "$2/$1/$3") );;
				var d = new Date( "01/"+b.replace( /(\d{2})\/(\d{2})\/(\d{4})/, "$2/$1/$3") );;
				return c-d;
			});

			//formatts date to mm/dd/yyyy and sorts them.
			dates.sort(function(a,b){
				var c = new Date( a.replace( /(\d{2})\/(\d{2})\/(\d{4})/, "$2/$1/$3") );;
				var d = new Date( b.replace( /(\d{2})\/(\d{2})\/(\d{4})/, "$2/$1/$3") );;
				return c-d;
			});

			for(mi=0;mi<months.length;mi++){
				chartObject.highChartsData.xAxis.categories.push(monthDisplay[parseInt(months[mi].split("/")[0])]);
          		chartObject.highChartsData.series[0].data.push(Number(trends[trendKeys[i]].monthly[months[mi]]));
			}
			for(di=0;di<dates.length;di++){
				var tmpDate = dates[di].split("/");
				dailyChart.highChartsData.xAxis.categories.push(tmpDate[0]+dateSuffix[parseInt(tmpDate[0])]+" "+monthDisplay[parseInt(tmpDate[1])]);
          		dailyChart.highChartsData.series[0].data.push(Number(trends[trendKeys[i]].daily[dates[di]]));
			}
			chartObjects.push(chartObject);
        	dailyCharts.push(dailyChart);
        	if(i+1==trendKeys.length){
        		d.resolve({trends:chartObjects,daily:dailyCharts});
        	}
    	}
    }
  return d.promise;
}

function processReportee(data){
	var d = Q.defer();
	var subviews = {
	    type: 'ol',
	    title: 'Reportee',
	    name: 'reporteedata',
	    content: []
	  },reporteeQuery=[];
	var reportee = data.reporteedata;
	if(reportee){
		while(reportee.length>0){
			var report = reportee.shift();
			report["type"] = "li";
			report["vpath"] = report["level"]+":"+report["code"];
			subviews.content.push(report);
			reporteeQuery.push(report["vpath"]);
			if(reportee.length==0){
				d.resolve({reportee:subviews,reporteeQuery:reporteeQuery});
			}
		}
	}else{
		d.resolve({reportee:null,reporteeQuery:reporteeQuery});
	}
	return d.promise;
}

function processObjects(message){
  var d = Q.defer();
  //console.log(message);
  Db.MongoClient.connect(databaseUrl, function(err, db) {
    //console.log(err);
    if(err) {
    	console.log(err);
    	throw err;
    }
    var body = JSON.parse(message.body.body)["data"];
    var parentLevel = message.body.parentLevel;

    //Initializing a collection on bulk Operations, implemented from mongodb 2.2
    //Automatically splits the records in to 1000 each and operates over it.

    var batch = db.collection("views").initializeUnorderedBulkOp();
    //console.log("Connected to Mongo");

    var codes = Object.keys(body);
    for(i=0;i<codes.length;i++){
      (function(i){
      	// Process Target and Achievement
	      //console.log(body[codes[i]]);
	      //console.log("Check 1 "+parentLevel+":"+codes[i]);
	      var processedData = {
				    _id:parentLevel+":"+codes[i] ,
				    vid:parentLevel+":"+codes[i] ,
				    lastSyncDate: new Date().toISOString(),
				    status: 'ready',
				    type: 'system',
				    resourceTemplate: 'BRITINDIA-SALES-TGTACH-1',
				    name: 'Sales Target vs Achievement Report',
				    header: {}, body: []
				  };

			function taillingProcessTargetVsAchievement(data){
				processedData.body.push(data.table);
				processedData.tmpSummary = data.summaryTmp;
			}

			function taillingProcessTrends(data){
				processedData.rawData ={
		      		"chartObjects":data.trends,
		      		"dailyCharts" : data.daily
		      	};
			}

			function taillingProcessReportee(data){
				processedData.body.push({
			            type: 'tables',
			            title: 'Summary Report',
			            subTitle: 'Values in Thousand(s)',
			            name: 'summary',
			            content: [
			            {
		                    "data": [
		                    ],
		                    "name": "salesdata",
		                    "title": "Sales (000)",
		                    "type": "table"
		                },
		                {
		                    "data": [
		                    ],
		                    "name": "distdata",
		                    "title": "Dist",
		                    "type": "table"
		                },
		                {
		                    "data": [
		                    ],
		                    "name": "edgedata",
		                    "title": "EDGE",
		                    "type": "table"
		                }]
		        	});
		        	processedData.body.push(data.reportee);
			      	view = {
			      		vid : processedData.vid,
			      		_id : processedData.vid,
			      		tmpSummary : processedData.tmpSummary,
			      		rawData : processedData.rawData,
			      		processedData : processedData,
			      		reporteeQuery : data.reporteeQuery
			      	};//reporteeQuery
              britCache.push(view.vid);
              //britCache[view._id] = view;
			      	batch.insert(view);
			      	//console.log("View Created : "+view.id);
			      	if(i+1 == codes.length){
			           console.log("Batch Executing");
			           batch.execute(function(err,result){
			           	db.close();
			           	d.resolve({msg:codes.join(',')+" saved."})
			           });
			        }
			}

	      processTargetVsAchievement(body[codes[i]])
	      .then(taillingProcessTargetVsAchievement)
	      .then(processTrends.bind(this,body[codes[i]]))
	      .then(taillingProcessTrends)
	      .then(processReportee.bind(this,body[codes[i]]))
	      .then(taillingProcessReportee)
      })(i);
    }
  });
  return d.promise;
}

// function is mandatory, has access to the req object for extra details
// it MUST return a promise, use whatever
function acceptData(message,headers) {
  var d = Q.defer()
  var body  = message.body;
  processObjects(message).then(function(msg){
    //console.log(msg);
    if(body.final){
    	console.log("Processing Extracted Data Done");
    	//[todo]
    	//maping user to view
    	d.resolve({msg:msg,note:"cache",final:body.final,cache:britCache});
    }
    else{
    	d.resolve({msg:msg,note:"cache",final:body.final});	
    }
    
  });
  return d.promise;
}
