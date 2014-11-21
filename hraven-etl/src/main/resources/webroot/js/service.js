$(document).ready(function() {
  
function timeConverter(UNIX_timestamp){
  var a = new Date(UNIX_timestamp);
  var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
  var year = a.getFullYear();
  var month = months[a.getMonth()];
  var date = a.getDate();
  var hour = a.getHours();
  var min = a.getMinutes();
  var sec = a.getSeconds();
  var time = ''+year+'/'+month+'/'+date+'/'+hour+':'+min;
  return time;
}
  
   $.getJSON("http://192.168.56.201:8080/api/v1/job/NCHC/hdadm", function(json){
		var jobName = [];
		var runTime = [];
		var cost = [];
		var megabyteMillis = [];
		
		
        for(var k in json) {
			jobName[k] = json[k]['jobName'];
			runTime[k] =json[k]['runTime'];
			cost[k]=json[k]['cost'];
			megabyteMillis[k]=json[k]['megabyteMillis'];
		}
		
		var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container1',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },
                              
                               
            xAxis: {
                categories: jobName,
                title: {
                    text: 'Job Name'
                }
            },
            yAxis: {
                title: {
                    text: 'COST (NTD)'
                },
				plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }],
				min: 0
				
            },
            tooltip: {
                formatter: function() {
					return this.x +'<br/>'+ this.y+' NTD.';
                    }
                },
                               
            series: [{
                name: 'COST',
                data: cost,
				color: '#FF0000'
            }]
        });	
		
		

		
		var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container2',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },
            xAxis: {
                categories: jobName,
                title: {
                    text: 'Job Name'
                }
            },
            yAxis: {
                title: {
                    text: 'Time (ms)'
                },
				plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function() {
					return this.x +'<br/>'+ this.y/1000 +' sec.';
                    }
                },
                               
            series: [{
                name: 'runTime',
                data: runTime,
				color: '#00FF00'
            }]
        });		
		
		var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container3',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },
                              
                               
            xAxis: {
                categories: jobName,
                title: {
                    text: 'Job Name'
                }
            },
            yAxis: {
                title: {
                    text: 'RAM X TIME <br> megabyteMillis)'
                },
				plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function() {
					return this.x +'<br/>'+ this.y/1000 +' MBSec.';
                    }
                },
                               
            series: [{
                name: 'megabyteMillis',
                data: megabyteMillis,
				color: '#0000FF'
            }]
        });	
    });

	


  	$.getJSON("http://192.168.56.201:8080/api/v1/job/NCHC/hdadm/TeraGen", function(json){
		var submitDate = [];
		var runTime = [];
		var megabyteMillis =[];
		var cost =[];
		for(var k in json) {
			
			var ts = json[k]['submitDate'];
			var ts2 = timeConverter(ts);			
			submitDate[k] = ts2;
			runTime[k] = json[k]['runTime'];
			megabyteMillis[k] = json[k]['megabyteMillis'];	
			cost[k] = json[k]['cost'];
		}

		
		var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container4',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },
                              
                               
            xAxis: {
                categories: submitDate,
                title: {
                    text: 'submit Time'
                }
            },
            yAxis: {
                title: {
                    text: 'COST (NTD)'
                },
				plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }],
				min: 0
				
            },
            tooltip: {
                formatter: function() {
					return this.x +'<br/>'+ this.y+' NTD.';
                    }
                },
                               
            series: [{
                name: 'COST',
                data: cost,
				color: '#FF0000'
            }]
        });	
		
		
		var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container4',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },
            xAxis: {
                categories: submitDate,
                title: {
                    text: 'submit Time'
                }
            },
            yAxis: {
                title: {
                    text: 'Time (ms)'
                },
				plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function() {
					return this.x +'<br/>'+ this.y/1000 +' sec.';
                    }
                },
                               
            series: [{
                name: 'runTime',
                data: runTime,
				color: '#00FF00'
            }]
        });		
		
		
		var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container6',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },
                              
                               
            xAxis: {
                categories: submitDate,
                title: {
                    text: 'submit time'
                }
            },
            yAxis: {
                title: {
                    text: 'RAM X TIME <br> megabyteMillis)'
                },
				plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function() {
					return this.x +'<br/>'+ this.y/1000 +' MBSec.';
                    }
                },
                               
            series: [{
                name: 'megabyteMillis',
                data: megabyteMillis,
				color: '#0000FF'
            }]
        });	
		
	});
});