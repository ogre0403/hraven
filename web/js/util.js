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
    if(sl < len)
        return orig;
    else
        return orig.substring(0,len-3)+'...';
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