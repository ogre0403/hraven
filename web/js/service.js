$(document).ready(function() {

    // share variable
    var CLUSTER = "NCHC";
    // change to yarn.nchc.org.tw in production, cookie only pass to ws iff in .nchc.org.tw domain
    var SERVER = "https://yarn.nchc.org.tw:8443/"
    var RESTPREFIX = "api/jsonp/";
    var dataarray;

    // UI
    $("#main").tabs();
    $("#typeToggle").buttonset();
    $("#typeToggle2").buttonset();
    // timepicker
    $(".datetime_selector").datetimepicker().button();
    // input user
    $(".user_input").button();
    $(".user_input_box").selectmenu();
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
                dataType:'jsonp',
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
                dataType:'jsonp',
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
                dataType:'jsonp',
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
                dataType:'jsonp',
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
                url: SERVER+RESTPREFIX+"jobList/"+CLUSTER+"/"+user,
                dataType:'jsonp',
                success: function(json){
                    updateMenu(json,"#job");
                }
            });
        }
    });

    function queryUser(user, start_ts,end_ts){
        var ts1 = convertToTS(start_ts);
        var ts2 = convertToTS(end_ts);

        dataarray =[[],[],[]]; //clear query result array

        $.getJSON(SERVER+RESTPREFIX+"job/"+CLUSTER+"/"+user+"/?start="+ts1+"&end="+ts2+"&callback=?", function(json){
            if(json.length == 0)
                alert("No Matched Result")

            for(var k in json) {
                dataarray[0][k] = shortStr(json[k]['jobName'],20);
                dataarray[1][k]  = Math.round(json[k]['runTime']/1000);
                dataarray[2][k]  = Math.round(json[k]['megabyteMillis']/(10*60*60))/100;
            }

            var para = {
                xtitle: "Job Name",
                ytitle: "Time (sec.)",
                series: "runTime",
                tip: " sec.",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[1],para);
        });
    }

    function queryJob(user,job,start_ts,end_ts){
        var ts1 = convertToTS(start_ts);
        var ts2 = convertToTS(end_ts);
        dataarray =[[],[],[],[]]; //clear query result array
         $.getJSON(SERVER+RESTPREFIX+"job/"+CLUSTER+"/"+user+"/"+job+"/?start="+ts1+"&end="+ts2+"&callback=?", function(json){

             if(json.length == 0)
                alert("No Matched Result")

             for(var k in json) {
                 dataarray[0][k]  = timeConverter(json[k]['submitTime']);
                 dataarray[1][k]  = Math.round(json[k]['runTime']/1000);
                 dataarray[2][k]  = Math.round(json[k]['megabyteMillis']/(10*60*60))/100;
             }

             var para = {
                xtitle: "submit Time",
                ytitle: "Time (sec.)",
                series: "runTime",
                tip: " sec.",
                 canvas: "container2"
             };
             plot(dataarray[0],dataarray[1],para);
         });
    }

    function queryJobDetail(run){
        $.ajax({
            url: SERVER+RESTPREFIX+"job/"+CLUSTER+"/?jobId="+run,
            dataType:'jsonp',
            success: function(json){
                detailTable2(json);
            }
        });
    }

    function queryRunningDetail(run){
        $.ajax({
            url: SERVER+RESTPREFIX+"running/status/"+run,
            dataType:'jsonp',
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

         $('#detailTable tr:gt(3)').remove();

         if(json['totalReduces'] == '-1'){
            $('#detailTable tr:last').after('<tr><th>Executor</th><td> </td></tr>');
         }else{
            $('#detailTable tr:last').after('<tr><th>Total Maps</th><td> </td><th>Total Reduces</th>	<td> </td></tr>');
            $('#detailTable tr:last').after('<tr><th>Finished Maps</th><td> </td><th>Finished Reduces</th><td> </td></tr>');
            $('#detailTable tr:last').after('<tr><th>Failed Maps</th><td> </td><th>Failed Reduces</th><td> </td></tr></tr>');
         }



        // value of megabyteMillis is equal to cores*millis
        // normalized to NCHC SU(cores * hours) by dividing 1000*60*60
        // in order to accurate to the second decimal place,
        // round(su/(10*60*60))/100
        SU = Math.round(json['megabyteMillis']/(10*60*60))/100;
        run_time = exec_duration(json['runTime']);
        submitTime = timeConverter(json['submitTime']);
        finishTime = timeConverter(json['finishTime']);

        $("#detailTable table tbody").children().eq(0).children().eq(1).replaceWith("<td>"+json['jobName']+"</td>"); // Jobname
        $("#detailTable table tbody").children().eq(0).children().eq(3).replaceWith("<td>"+json['status']+"</td>"); // Job status
        $("#detailTable table tbody").children().eq(1).children().eq(1).replaceWith("<td>"+json['user']+"</td>"); // User
        $("#detailTable table tbody").children().eq(1).children().eq(3).replaceWith("<td>"+json['queue']+"</td>"); // Queue
        $("#detailTable table tbody").children().eq(2).children().eq(1).replaceWith("<td>"+submitTime+"</td>"); // submit time
        $("#detailTable table tbody").children().eq(2).children().eq(3).replaceWith("<td>"+finishTime+"</td>"); // finish time
        $("#detailTable table tbody").children().eq(3).children().eq(1).replaceWith("<td>"+run_time+"</td>"); // run time
        $("#detailTable table tbody").children().eq(3).children().eq(3).replaceWith("<td>"+SU+"</td>"); // cost

        if(json['totalReduces'] == '-1'){
            $("#detailTable table tbody").children().eq(4).children().eq(1).replaceWith("<td>"+json['totalMaps']+"</td>"); // total map
        }else{
            $("#detailTable table tbody").children().eq(4).children().eq(1).replaceWith("<td>"+json['totalMaps']+"</td>"); // total map
            $("#detailTable table tbody").children().eq(4).children().eq(3).replaceWith("<td>"+json['totalReduces']+"</td>"); // total reduce
            $("#detailTable table tbody").children().eq(5).children().eq(1).replaceWith("<td>"+json['finishedMaps']+"</td>"); // finish map
            $("#detailTable table tbody").children().eq(5).children().eq(3).replaceWith("<td>"+json['finishedReduces']+"</td>"); // finish reduce
            $("#detailTable table tbody").children().eq(6).children().eq(1).replaceWith("<td>"+json['failedMaps']+"</td>"); // failed map
            $("#detailTable table tbody").children().eq(6).children().eq(3).replaceWith("<td>"+json['failedReduces']+"</td>"); // failed reduce
        }

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

    $("#btntime").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "Time (sec.)",
                series: "runTime",
                tip: " sec.",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[1],para);
        }
    );
    $("#btntime1").click(function () {
            var para = {
                xtitle: "submit Time",
                ytitle: "Time (sec.)",
                series: "runTime",
                tip: " sec.",
                canvas: "container2"
            };
            plot(dataarray[0],dataarray[1],para);
        }
    );
    $("#btnmbms").click(function () {
            var para = {
                xtitle: "Job Name",
                ytitle: "SU (cores-hours)",
                series: "SU",
                tip: " SU",
                canvas: "container1"
            };
            plot(dataarray[0],dataarray[2],para);
        }
    );

    $("#btnmbms1").click(function () {
            var para = {
                xtitle: "submit Time",
                ytitle: "SU (cores-hours)",
                series: "SU",
                tip: " SU",
                canvas: "container2"
            };
            plot(dataarray[0],dataarray[2],para);
        }
    );

// JSONP sample

    $("#qtest").click(function(){
            testjsonp1();
    });


    function testjsonp1(){
        $.ajax({
            url: 'http://192.168.56.201:8080/api/jsonp/jsonp3',
            dataType: 'jsonp',
            success: function(res){
                         jsonpCallbackk(res);
            },
            error: function(){
               alert('fail');
            }
        });
    }

    function jsonpCallbackk(data){
        console.log(data[1]['mapProgress']);
    }


});