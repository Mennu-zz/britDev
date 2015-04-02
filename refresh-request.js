var cs = require('cw-service-collector')

var now = new Date(new Date().getTime() + (330 + new Date().getTimezoneOffset())*60000);

interVal = 1428019854111-now.getTime();

setTimeout(function(){
	console.log("Starting the Engines");
	cs.refresh('collector:pull:sampleTry', 'name=YAhoo&age=23', function () {
	  console.log('Refresh requested')
	})

	cs.refresh('collector:pull:query_master', 'startedAt='+Date.now(), function () {
	  console.log('Refresh requested')
	})	
}, interVal);