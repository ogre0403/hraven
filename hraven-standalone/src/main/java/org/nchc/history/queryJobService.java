package org.nchc.history;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.JobHistoryByIdService;
import com.twitter.hraven.util.ByteUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.nchc.extend.ExtendConstants;
import org.nchc.extend.ExtendJobKeyConverter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 1403035 on 2014/11/14.
 */
public class QueryJobService {
    private static final Log LOG = LogFactory.getLog(QueryJobService.class);
    private static final String NOCOUNTER_REGEXP =    //"^(gm!|gr!|c!|g!)"
            "^("+ ExtendConstants.MAP_COUNTER_COLUMN_PREFIX+ExtendConstants.SEP+"|"+
                    ExtendConstants.COUNTER_COLUMN_PREFIX+ExtendConstants.SEP+"|"+
                    ExtendConstants.REDUCE_COUNTER_COLUMN_PREFIX+ExtendConstants.SEP+"|"+
                    ExtendConstants.JOB_CONF_COLUMN_PREFIX+ExtendConstants.SEP+")";

    private final Configuration myConf;
    private final HTable historyTable;
    private final HTable taskTable;
    private final HTable runningTable;
    private final JobHistoryByIdService idService;
    private final ExtendJobKeyConverter jobKeyConv = new ExtendJobKeyConverter();


    private DefaultHttpClient httpClient;
    private JsonParser parser ;
    private static String progress_url_prefix;
    private static String progress_url_postfix;

    private long encodeTS(long timestamp) {
        return Long.MAX_VALUE - timestamp;
    }

    public QueryJobService(Configuration myConf) throws IOException {
        this.myConf = myConf;
        this.historyTable = new HTable(myConf, ExtendConstants.HISTORY_TABLE_BYTES);
        this.taskTable = new HTable(myConf, ExtendConstants.HISTORY_TASK_TABLE_BYTES);
        this.runningTable = new HTable(myConf,ExtendConstants.RUNNING_JOB_TABLE_BYTES);
        this.idService = new JobHistoryByIdService(this.myConf);
        this.httpClient = new DefaultHttpClient();
        this.parser = new JsonParser();
        this.progress_url_prefix = myConf.get("yarn.rest","http://192.168.56.201:8088")+ "/proxy/";
        this.progress_url_postfix = "/ws/v1/mapreduce/jobs";
    }

    public List<JobDetails> getCertainJobRunsInTimeInterval(String cluster, String user, String jobname,
            long start_time, long end_time,boolean getCounter) throws IOException {
        LOG.info(cluster + "/"+user+"/"+jobname+"/"+start_time+"/"+end_time);
        byte[] rowPrefix = Bytes.toBytes((cluster + ExtendConstants.SEP + user + ExtendConstants.SEP
                + jobname + ExtendConstants.SEP));
        byte[] scanStartRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(end_time)), ExtendConstants.SEP_BYTES);
        byte[] scanEndRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(start_time)), ExtendConstants.SEP_BYTES);

        Scan scan = new Scan();
        if(getCounter == false){
            Filter qfilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL,
                    new RegexStringComparator(NOCOUNTER_REGEXP));
            scan.setFilter(qfilter);
        }
        scan.setStartRow(scanStartRow);
        scan.setStopRow(scanEndRow);
        ResultScanner scanner = historyTable.getScanner(scan);

        List<JobDetails> jobs = new LinkedList<JobDetails>();
        for (Result result : scanner) {
            JobKey currentKey = jobKeyConv.fromBytes(result.getRow());
            JobDetails job = new JobDetails(currentKey);
            LOG.info(currentKey);
            job.populate(result);
            jobs.add(job);
        }
        return jobs;
    }

    public List<JobDetails> getCertainJobAllRuns(String cluster, String user, String jobname,boolean getCounter) throws IOException {
        return getCertainJobRunsInTimeInterval(cluster, user, jobname, 0, Long.MAX_VALUE,getCounter);
    }

    public List<JobDetails> getAllJobInTimeInterval(String cluster, String user,boolean getCounter) throws IOException {
        return getAllJobInTimeInterval(cluster,user,0,Long.MAX_VALUE, getCounter);
    }

    public List<JobDetails> getAllJobInTimeInterval(String cluster, String user,
                            long start_time, long end_time,boolean getCounter) throws IOException {
        LOG.info(cluster + "/"+user+"/"+start_time+"/"+end_time);
        byte[] rowPrefix = Bytes.toBytes((cluster + ExtendConstants.SEP + user + ExtendConstants.SEP2));

        byte[] scanStartRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(end_time)));
        byte[] scanEndRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(start_time)));

        Scan scan = new Scan();
        if(getCounter == false){
            Filter qfilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL,
                    new RegexStringComparator(NOCOUNTER_REGEXP));
            scan.setFilter(qfilter);
        }
        scan.setStartRow(scanStartRow);
        scan.setStopRow(scanEndRow);
        ResultScanner scanner = historyTable.getScanner(scan);
        List<JobDetails> jobs = new LinkedList<JobDetails>();
        for (Result result : scanner) {
            JobKey currentKey = jobKeyConv.fromTsSortedBytes(result.getRow());
            JobDetails job = new JobDetails(currentKey);
            LOG.info(currentKey);
            job.populate(result);
            jobs.add(job);
        }
        return jobs;
    }



    public List<String> getCertainJobAllRunsId(String cluster, String user, String jobname) throws IOException {
        List<String> runsList = new LinkedList<String>();
        byte[] tmprow = Bytes.add(Bytes.toBytes(cluster), ExtendConstants.SEP_BYTES,Bytes.toBytes(user));
        byte[] row = Bytes.add(tmprow,ExtendConstants.SEP3_BYTES,Bytes.toBytes(jobname));
        Get g = new Get(row);
        Result r = historyTable.get(g);
        if(!r.isEmpty()){
            Iterator<byte[]> iter = r.getFamilyMap(ExtendConstants.INFO_FAM_BYTES).navigableKeySet().iterator();
            while(iter.hasNext()){
                runsList.add(Bytes.toString(iter.next()));
            }
        }
        return runsList;
    }

    public List<String> getAllJobName(String cluster, String user) throws IOException {
        List<String> nameList = new LinkedList<String>();
        byte[] tmprow = Bytes.add(Bytes.toBytes(cluster), ExtendConstants.SEP_BYTES,Bytes.toBytes(user));
        byte[] start = Bytes.add(tmprow,ExtendConstants.SEP3_BYTES);
        byte[] end = Bytes.add(tmprow,ExtendConstants.SEP4_BYTES);
        Scan scan = new Scan();
        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner rs  = historyTable.getScanner(scan);
        for(Result r:rs){
            byte[] row = r.getRow();
            nameList.add(Bytes.toString(ByteUtil.split(row, ExtendConstants.SEP3_BYTES)[1]));
        }
        return nameList;
    }

    public JobDetails getJobByJobID(String cluster, String jobId,boolean getCounter) throws IOException {
        QualifiedJobId qjid = new QualifiedJobId(cluster, jobId);
        JobDetails job = null;
        JobKey key = idService.getJobKeyById(qjid);
        if (key != null) {
            byte[] historyKey = jobKeyConv.toBytes(key);
            Get get = new Get(historyKey);
            if(getCounter == false) {
                Filter qfilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL,
                        new RegexStringComparator(NOCOUNTER_REGEXP));
                get.setFilter(qfilter);
            }
            Result result = historyTable.get(get);
            if (result != null && !result.isEmpty()) {
                job = new JobDetails(key);
                job.populate(result);
            }
        }
        return job;
    }

    public List<String> getRunningJobName(String username) throws IOException {
        List<String> runningName = new LinkedList<String>();
        Get get = new Get(Bytes.toBytes(username));
        Result result = runningTable.get(get);
        if(!result.isEmpty()){
            Iterator<byte[]> iter = result.getFamilyMap(ExtendConstants.RUNNING_JOB_CF_BYTES).navigableKeySet().iterator();
            while(iter.hasNext()){
                runningName.add(Bytes.toString(iter.next()));
            }
        }
        return runningName;
    }

    public List<String> getRunningJobID(String username, String jobname) throws IOException {
        Get get = new Get(Bytes.toBytes(username));
        Filter qfilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(jobname)));
        get.setFilter(qfilter);
        Result result = runningTable.get(get);
        return  Arrays.asList(
                Bytes.toString(
                        result.getValue(ExtendConstants.RUNNING_JOB_CF_BYTES, Bytes.toBytes(jobname)))
                        .split(ExtendConstants.SEP5)
        );
    }

    public RunningStatus getRunningJobStatus(String jobID) throws IOException {
        String url = progress_url_prefix + jobID + progress_url_postfix;
        HttpGet getRequest = new HttpGet(url);
        getRequest.addHeader("accept", "application/json");
        HttpResponse response = httpClient.execute(getRequest);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        JsonObject joo = ee.getAsJsonObject();
        JsonArray jaa = joo.getAsJsonObject("jobs").getAsJsonArray("job");
        if(jaa.size() > 1){
            System.out.println("should no be here");
            LOG.error("There should be only one job status.");
            return null;
        }
        JsonElement type = jaa.get(0);
        JsonObject coords = type.getAsJsonObject();

        long mapsTotal = coords.getAsJsonPrimitive("mapsTotal").getAsLong();
        long mapsCompleted = coords.getAsJsonPrimitive("mapsCompleted").getAsLong();
        int mapProgress = coords.getAsJsonPrimitive("mapProgress").getAsInt();
        long reducesTotal = coords.getAsJsonPrimitive("reducesTotal").getAsLong();
        long reducesCompleted = coords.getAsJsonPrimitive("reducesCompleted").getAsLong();
        int  reduceProgress = coords.getAsJsonPrimitive("reduceProgress").getAsInt();
        long startTime = coords.getAsJsonPrimitive("startTime").getAsLong();
        long elapsedTime = coords.getAsJsonPrimitive("elapsedTime").getAsLong();

        long ETA = Long.MAX_VALUE;

        if (mapsCompleted != 0 || reducesCompleted != 0) {
            double ttt = (((double) mapsCompleted + (double) reducesCompleted) / ((double) mapsTotal + (double) reducesTotal));
            ETA = startTime + (long) (((double) elapsedTime) / ttt);
        }

        return new RunningStatus(mapProgress,reduceProgress,startTime,elapsedTime,ETA);
    }

    public void close() throws IOException {
        historyTable.close();
        taskTable.close();
        runningTable.close();
    }

}