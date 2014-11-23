$(document).ready(function() {
    // UI
    $("#main").tabs();
    $("#startdatetimepicker1").datetimepicker().button();
    $("#enddatetimepicker").datetimepicker().button();
    $("#typeToggle").buttonset();

    $("#jobuser").button();
    $("#qb").button();
    $("#job").button();
    $("#jobst").datetimepicker().button();
    $("#jobet").datetimepicker().button();
    $("#typeToggle2").buttonset();

    function timeConverter(UNIX_timestamp){
      var a = new Date(UNIX_timestamp);
      var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
      var year = a.getFullYear();
      var month = months[a.getMonth()];
      var date = a.getDate();
      var hour = a.getHours();
      var min = a.getMinutes();
      //var sec = a.getSeconds();
      var time = ''+year+'/'+month+'/'+date+'/'+hour+':'+min;
      return time;
    }

    function convertToTS(HumanTime){
        var d = HumanTime.match(/\d+/g); // extract date parts
        return +new Date(d[0], d[1] - 1, d[2], d[3], d[4], d[5]); // build Date object
    }

    function queryUser(user, start_ts,end_ts){
        alert(user + start_ts + end_ts);
        //var ts = convertToTS("2014/11/22 12:59:00");
        //console.log(ts);
        var para = {
            xtitle: "Job Name",
            ytitle: "COST (NTD)",
            series: "COST",
            tip: " NTD."
        };
        var items = [["JobA","JobB","JobC"],[100,200,300],[1,2,3],[0.1,0.2,0.3]];
        plot(items[0],items[1],para);
        return items;

        //TODO: return 2d array
        //['X-axis'][...]
        //['cost']  [...]
        //['time']  [...]
        //['MbMs']  [...]
    }

    function queryJob(user,job,start_ts,end_ts){
        alert(user + job + start_ts + end_ts);
        //TODO: return 2d array
        //['X-axis'][...]
        //['cost']  [...]
        //['time']  [...]
        //['MbMs']  [...]
    }

    function plot(Xaxis, Yaxis, showPara){
        var chart = new Highcharts.Chart({
            chart: {
                renderTo: 'container1',
                defaultSeriesType: 'line',
                margin: [50, 150, 60, 80]
            },

            xAxis: {
                categories: Xaxis,
                title: {
                    text: showPara.xtitle
                }
            },
            yAxis: {
                title: {
                    text: showPara.ytitle
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
                    return this.x +'<br/>'+ this.y+ showPara.tip;
                }
            },

            series: [{
                name: showPara.series,
                data: Yaxis,
                color: '#FF0000'
            }]
        });
    }


    var dataarray;
    $("#user").button();
    $("#qqq").button().click(function(){
        var v1 = $("#user").val();
        var v2 = $("#startdatetimepicker1").val();
        var v3 = $("#enddatetimepicker").val();
        dataarray = queryUser(v1,v2,v3);
    });

    $("#btncost").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "COST (NTD)",
                series: "COST",
                tip: " NTD."
            };
            plot(dataarray[0],dataarray[1],para);
        }
    );

    $("#btntime").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "Time (ms)",
                series: "runTime",
                tip: " sec."
            };
            plot(dataarray[0],dataarray[2],para);
        }
    );

    $("#btnmbms").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "RAM X TIME <br> (megabyteMillis)",
                series: "megabyteMillis",
                tip: " MBSec."
            };
            plot(dataarray[0],dataarray[3],para);
        }
    );




});