$(document).ready(function() {

    // share variable
    var CLUSTER = "NCHC";
    var SERVER = "http://192.168.56.201:8080/"
    var RESTPREFIX = "api/v1/";
    var dataarray;

    // UI
    $("#main").tabs();
    $("#typeToggle").buttonset();
    $("#typeToggle2").buttonset();
    // timepicker
    $(".datetime_selector").datetimepicker().button();
    // input user
    $(".user_input").button();
    //buttom
    $(".query_button").button();


    function updateMenu(json,str_id){
        $(str_id).empty();
        if (json.length > 0){
            $.each(json, function(key,value) {
               $(str_id)
                .append($("<option></option>")
                .attr("value",value)
                .text(value));
            });
        }else{
            $(str_id).append($("<option></option>").attr("value","Not Found").text("Not Found"));
        }
        $(str_id).selectmenu('refresh');
    }

    $("#runningjoblist").selectmenu({
        open: function( event, ui ) {
            var user = $("#runningusername").val();
            $.ajax({
                url: SERVER+RESTPREFIX+"running/job/"+user,
                type:"GET",
                dataType:'json',
                success: function(json){
                  updateMenu(json, "#runningjoblist");
                }
            });
        }
    });

    $("#runningidlist").selectmenu({
        open: function( event, ui ) {
            var user = $("#runningusername").val();
            var job = $("#runningjoblist").val();
            $.ajax({
                url: SERVER+RESTPREFIX+"running/id/"+user+"/"+job,
                type:"GET",
                dataType:'json',
                success: function(json){
                  updateMenu(json, "#runningidlist");
                }
            });
        }
    });

    $("#joblist").selectmenu({
        open: function( event, ui ) {
            var user = $("#user3").val();
            $.ajax({
                url: SERVER+RESTPREFIX+"jobList/"+CLUSTER+"/"+user,
                type:"GET",
                dataType:'json',
                success: function(json){
                  updateMenu(json, "#joblist");
                }
            });
        }
    });

    $("#runlist").selectmenu({
        open: function( event, ui ) {
            var user = $("#user3").val();
            var job = $("#joblist").val();
             $.ajax({
                url: SERVER+RESTPREFIX+"runList/"+CLUSTER+"/"+user+"/"+job,
                type:"GET",
                dataType:'json',
                success: function(json){
                  updateMenu(json, "#runlist");
                }
             });
        }
    });

    $("#job").selectmenu({
        open: function( event, ui ) {
            var user = $("#jobuser").val();
            $.ajax({
//                url: "http://192.168.56.201:8080/api/v1/jobList/"+CLUSTER+"/"+user,
                url: SERVER+RESTPREFIX+"jobList/"+CLUSTER+"/"+user,
                type:"GET",
                dataType:'json',
                success: function(json){
                    updateMenu(json,"#job");
                }
            });
        }
    });

    function exec_duration(ms_duration){
        t = Math.floor(ms_duration/1000);;
        h=""+(t/36000|0)+(t/3600%10|0);
        m=""+(t%3600/600|0)+(t%3600/60%10|0);
        s=""+(t%60/10|0)+(t%60%10);
        T=h+"時"+m+"分"+s+"秒";
        return T;
    }

    function timeConverter(UNIX_timestamp){
      var a = new Date(UNIX_timestamp);
      var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
      var year = a.getFullYear();
      var month = months[a.getMonth()];
      var date = a.getDate();
      var hour = a.getHours();
      var min = a.getMinutes();
      var sec = a.getSeconds();
      var time = ''+year+'/'+month+'/'+date+'\n'+hour+':'+min+":"+sec;
      return time;
    }

    function convertToTS(HumanTime){
        // input format from DateTimepicker is mm/dd/yyyy hh:mm
        // However, Date() ctor is
        // Date(year, month, day, hours, minutes, seconds, milliseconds);
        var d = HumanTime.match(/\d+/g); // extract date parts
        if(d == null)
            return -1;
        return +new Date(d[2], d[0] - 1, d[1], d[3], d[4], 0); // build Date object
    }


    function shortStr(orig, len){
        var sl = orig.length;
        console.log(sl);
        if(sl < len)
            return orig;
        else
            return orig.substring(0,len-3)+'...';
    }

    function queryUser(user, start_ts,end_ts){
        var ts1 = convertToTS(start_ts);
        var ts2 = convertToTS(end_ts);

        dataarray =[[],[],[],[]]; //clear query result array

        //http://192.168.56.201:8080/api/v1/job/<CLUSTER>/<USER>
        //$.getJSON(SERVER+RESTPREFIX+"job/"+CLUSTER+"/"+user, function(json){
        $.getJSON(SERVER+RESTPREFIX+"job/"+CLUSTER+"/"+user+"/?start="+ts1+"&end="+ts2, function(json){
            for(var k in json) {
                dataarray[0][k] = shortStr(json[k]['jobName'],13);
                dataarray[1][k]  =json[k]['cost'];
                dataarray[2][k] =json[k]['runTime'];
                dataarray[3][k]  = json[k]['megabyteMillis'];
            }

            var para = {
                xtitle: "Job Name",
                ytitle: "COST (NTD)",
                series: "COST",
                tip: " NTD.",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[1],para);
        });
    }

    function queryJob(user,job,start_ts,end_ts){
        var ts1 = convertToTS(start_ts);
        var ts2 = convertToTS(end_ts);
        dataarray =[[],[],[],[]]; //clear query result array
         $.getJSON(SERVER+RESTPREFIX+"job/"+CLUSTER+"/"+user+"/"+job+"/?start="+ts1+"&end="+ts2, function(json){
             for(var k in json) {
                 dataarray[0][k] = timeConverter(json[k]['submitTime']);
                 dataarray[1][k]  =json[k]['cost'];
                 dataarray[2][k] =json[k]['runTime'];
                 dataarray[3][k]  = json[k]['megabyteMillis'];
             }

             var para = {
                 xtitle: "submitTime",
                 ytitle: "COST (NTD)",
                 series: "COST",
                 tip: " NTD.",
                 canvas: "container2"
             };
             plot(dataarray[0],dataarray[1],para);
         });
    }

    function queryJobDetail(run){
        $.ajax({
            url: SERVER+RESTPREFIX+"job/"+CLUSTER+"/?jobId="+run,
            type:"GET",
            dataType:'json',
            success: function(json){
                detailTable2(json);
            }
        });
    }

    function queryRunningDetail(run){
        $.ajax({
            url: SERVER+RESTPREFIX+"running/status/"+run,
            type:"GET",
            dataType:'json',
            success: function(json){
                showRunningStatus(json);
            }
        });
    }


    function showRunningStatus(json){
        mapProgress= json['mapProgress']+"%";
        reduceProgress = json['reduceProgress']+"%";
        startTime = timeConverter(json['startTime']);
        elapsedTime = exec_duration(json['elapsedTime']);

        if(json['eta'] == '9223372036854776000'){
            eta = "N/A";
        }else if(json['eta'] == '0'){
            $("#runningstatustable table tbody").children().eq(0).children().eq(1).replaceWith("<td>"+"N/A"+"</td>");
            $("#runningstatustable table tbody").children().eq(0).children().eq(3).replaceWith("<td>"+"N/A"+"</td>");
            $("#runningstatustable table tbody").children().eq(1).children().eq(1).replaceWith("<td>"+"N/A"+"</td>");
            $("#map_progress").css('width', "0%");
            $("#reduce_progress").css('width', "0%");
            document.getElementById("map_progress_text").innerHTML = "0 %";
            document.getElementById("reduce_progress_text").innerHTML = "0 %";
            return;
        }else{
            eta = timeConverter(json['eta']);
        }

        $("#runningstatustable table tbody").children().eq(0).children().eq(1).replaceWith("<td>"+startTime+"</td>");
        $("#runningstatustable table tbody").children().eq(0).children().eq(3).replaceWith("<td>"+elapsedTime+"</td>");
        $("#runningstatustable table tbody").children().eq(1).children().eq(1).replaceWith("<td>"+eta+"</td>");
        $("#map_progress").css('width',mapProgress);
        $("#reduce_progress").css('width', reduceProgress);
        document.getElementById("map_progress_text").innerHTML = mapProgress;
        document.getElementById("reduce_progress_text").innerHTML = reduceProgress;
    }

    function detailTable2(json){
        cost = Math.round(json['cost']*100)/100;
        run_time = exec_duration(json['runTime']);
        submitTime = timeConverter(json['submitTime']);
        finishTime = timeConverter(json['finishTime']);

        $("#detailTable table tbody").children().eq(0).children().eq(1).replaceWith("<td>"+json['status']+"</td>"); // Jobname
        $("#detailTable table tbody").children().eq(0).children().eq(3).replaceWith("<td>"+json['jobName']+"</td>"); // Job status
        $("#detailTable table tbody").children().eq(1).children().eq(1).replaceWith("<td>"+json['user']+"</td>"); // User
        $("#detailTable table tbody").children().eq(1).children().eq(3).replaceWith("<td>"+json['queue']+"</td>"); // Queue
        $("#detailTable table tbody").children().eq(2).children().eq(1).replaceWith("<td>"+submitTime+"</td>"); // submit time
        $("#detailTable table tbody").children().eq(2).children().eq(3).replaceWith("<td>"+finishTime+"</td>"); // finish time
        $("#detailTable table tbody").children().eq(3).children().eq(1).replaceWith("<td>"+run_time+"</td>"); // run time
        $("#detailTable table tbody").children().eq(3).children().eq(3).replaceWith("<td>"+cost+"</td>"); // cost
        $("#detailTable table tbody").children().eq(4).children().eq(1).replaceWith("<td>"+json['totalMaps']+"</td>"); // total map
        $("#detailTable table tbody").children().eq(4).children().eq(3).replaceWith("<td>"+json['totalReduces']+"</td>"); // total reduce
        $("#detailTable table tbody").children().eq(5).children().eq(1).replaceWith("<td>"+json['finishedMaps']+"</td>"); // finish map
        $("#detailTable table tbody").children().eq(5).children().eq(3).replaceWith("<td>"+json['finishedReduces']+"</td>"); // finish reduce
        $("#detailTable table tbody").children().eq(6).children().eq(1).replaceWith("<td>"+json['failedMaps']+"</td>"); // failed map
        $("#detailTable table tbody").children().eq(6).children().eq(3).replaceWith("<td>"+json['failedReduces']+"</td>"); // failed reduce
    }

    function plot(Xaxis, Yaxis, showPara){
        var chart = new Highcharts.Chart({
            chart: {
                renderTo: showPara.canvas,
                defaultSeriesType: 'column',
                //margin: [50, 150, 60, 80]
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
                color: '#B22222'
            }]
        });
    }

    $("#q4").click(function(){
        var jobid = $("#runningidlist").val();
        queryRunningDetail(jobid);
    });



    $("#qqq").click(function(){
        var v1 = $("#user").val();
        var v2 = $("#startdatetimepicker1").val();
        var v3 = $("#enddatetimepicker").val();
        queryUser(v1,v2,v3);
    });

     $("#qb").click(function(){
            var v1 = $("#jobuser").val();
            var v2 = $("#job").val();
            var v3 = $("#jobst").val();
            var v4 = $("#jobet").val();
            queryJob(v1,v2,v3,v4);
     });

     $("#q3").click(function(){
         var v3 = $("#runlist").val();
         queryJobDetail(v3);
     });

    $("#btncost").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "COST (NTD)",
                series: "COST",
                tip: " NTD.",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[1],para);
        }
    );

    $("#btncost1").click(function () {
            var para = {
                xtitle: "submitTime",
                ytitle: "COST (NTD)",
                series: "COST",
                tip: " NTD.",
                canvas: "container2"
            };
            plot(dataarray[0],dataarray[1],para);
        }
    );

    $("#btntime").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "Time (ms)",
                series: "runTime",
                tip: " sec.",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[2],para);
        }
    );
    $("#btntime1").click(function () {
            var para = {
                xtitle: "submitTime",
                ytitle: "Time (ms)",
                series: "runTime",
                tip: " sec.",
                canvas: "container2"
            };
            plot(dataarray[0],dataarray[2],para);
        }
    );
    $("#btnmbms").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "RAM X TIME <br> (megabyteMillis)",
                series: "megabyteMillis",
                tip: " MBSec.",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[3],para);
        }
    );

    $("#btnmbms1").click(function () {
            var para = {
                xtitle: "submitTime",
                ytitle: "RAM X TIME <br> (megabyteMillis)",
                series: "megabyteMillis",
                tip: " MBSec.",
                canvas: "container2"
            };
            plot(dataarray[0],dataarray[3],para);
        }
    );
});