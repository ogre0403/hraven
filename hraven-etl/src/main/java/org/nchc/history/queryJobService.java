package org.nchc.history;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobHistoryByIdService;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.TaskKeyConverter;
import com.twitter.hraven.util.ByteUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 1403035 on 2014/11/14.
 */
public class queryJobService {
    private static final Log LOG = LogFactory.getLog(queryJobService.class);
    private final Configuration myConf;
    private final HTable historyTable;
    private final HTable taskTable;
    private final JobHistoryByIdService idService;
    private final JobKeyConverter jobKeyConv = new JobKeyConverter();
    private final TaskKeyConverter taskKeyConv = new TaskKeyConverter();

    private final int defaultScannerCaching;


    private long encodeTS(long timestamp) {
        return Long.MAX_VALUE - timestamp;
    }

    public queryJobService(Configuration myConf) throws IOException {
        this.myConf = myConf;
        this.historyTable = new HTable(myConf, Constants.HISTORY_TABLE_BYTES);
        this.taskTable = new HTable(myConf, Constants.HISTORY_TASK_TABLE_BYTES);
        this.idService = new JobHistoryByIdService(this.myConf);
        this.defaultScannerCaching = myConf.getInt("hbase.client.scanner.caching", 100);
    }

    // String jobID_str = job_1415006048688_000008;


    public List<JobDetails> getCertainJobRunsInTimeInterval(String cluster, String user, String jobname,
            long start_time, long end_time) throws IOException {
        LOG.info(cluster + "/"+user+"/"+jobname+"/"+start_time+"/"+end_time);
        byte[] rowPrefix = Bytes.toBytes((cluster + Constants.SEP + user + Constants.SEP
                + jobname + Constants.SEP));
        byte[] scanStartRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(end_time)), Constants.SEP_BYTES);
        byte[] scanEndRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(start_time)), Constants.SEP_BYTES);
        Scan scan = new Scan();
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

    public List<JobDetails> getCertainJobAllRuns(String cluster, String user, String jobname) throws IOException {
        return getCertainJobRunsInTimeInterval(cluster, user, jobname, 0, Long.MAX_VALUE);
    }

    public List<JobDetails> getAllJobInTimeInterval(String cluster, String user) throws IOException {
        return getAllJobInTimeInterval(cluster,user,0,Long.MAX_VALUE);
    }

    public List<JobDetails> getAllJobInTimeInterval(String cluster, String user, long start_time, long end_time) throws IOException {
        LOG.info(cluster + "/"+user+"/"+start_time+"/"+end_time);
        byte[] rowPrefix = Bytes.toBytes((cluster + Constants.SEP + user + Constants.SEP2));

        byte[] scanStartRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(end_time)));
        byte[] scanEndRow = Bytes.add(rowPrefix, Bytes.toBytes(encodeTS(start_time)));

        Scan scan = new Scan();
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
        byte[] tmprow = Bytes.add(Bytes.toBytes(cluster), Constants.SEP_BYTES,Bytes.toBytes(user));
        byte[] row = Bytes.add(tmprow,Constants.SEP3_BYTES,Bytes.toBytes(jobname));
        Get g = new Get(row);
        Result r = historyTable.get(g);
        if(!r.isEmpty()){
            Iterator<byte[]> iter = r.getFamilyMap(Constants.INFO_FAM_BYTES).navigableKeySet().iterator();
            while(iter.hasNext()){
                runsList.add(Bytes.toString(iter.next()));
            }
        }
        return runsList;
    }

    public List<String> getAllJobName(String cluster, String user) throws IOException {
        List<String> nameList = new LinkedList<String>();
        byte[] tmprow = Bytes.add(Bytes.toBytes(cluster), Constants.SEP_BYTES,Bytes.toBytes(user));
        byte[] start = Bytes.add(tmprow,Constants.SEP3_BYTES);
        byte[] end = Bytes.add(tmprow,Constants.SEP4_BYTES);
        Scan scan = new Scan();
        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner rs  = historyTable.getScanner(scan);
        for(Result r:rs){
            byte[] row = r.getRow();
            nameList.add(Bytes.toString(ByteUtil.split(row, Constants.SEP3_BYTES)[1]));
        }
        return nameList;
    }

    public void close() throws IOException {
        historyTable.close();
        taskTable.close();
    }
}
