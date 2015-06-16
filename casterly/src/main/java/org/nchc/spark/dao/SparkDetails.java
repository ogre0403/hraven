package org.nchc.spark.dao;


import com.twitter.hraven.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.nchc.extend.ExtendConstants;
import org.nchc.extend.ExtendJobHistoryService;
import org.nchc.extend.ExtendJobKeyConverter;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by 1403035 on 2015/6/10.
 */
public class SparkDetails {
    static Log LOG = LogFactory.getLog(SparkDetails.class);
    private JobKey jobKey;

    /** Job ID, minus the leading "job_" */
    private String jobNumber;
    private byte[] jobKeyBytes;
    private byte[] jobKeyByTS;
    private String jobStatus = "Completed";


    private String AppName;
    private String AppID;
    private long StartTimestamp;
    private long EndTimestamp;
    private int ExecutorCount;
    private String User;

    private JobDesc jobDesc_w_finishT;
    private JobDesc jobDesc_w_submitT;
    private ExtendJobKeyConverter jobKeyConv = new ExtendJobKeyConverter();

    public String getAppName() {
        return AppName;
    }

    public String getAppID() {
        return AppID;
    }

    public long getStartTimestamp() {
        return StartTimestamp;
    }

    public long getEndTimestamp() {
        return EndTimestamp;
    }

    public int getExecutorCount() {
        return ExecutorCount;
    }

    public String getUser() {
        return User;
    }

    public void setAppName(String appName) {
        AppName = appName;
    }

    public void setAppID(String appID) {
        AppID = appID;
    }

    public void setStartTimestamp(long startTimestamp) {
        StartTimestamp = startTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        EndTimestamp = endTimestamp;
    }

    public void setExecutorCount(int executorCount) {
        ExecutorCount = executorCount;
    }

    public void setUser(String user) {
        User = user;
    }

    public void setJobFailed(){
        this.jobStatus = "Failed";
    }

    public JobDesc getJobDesc_w_finishT() {
        return jobDesc_w_finishT;
    }

    public JobDesc getJobDesc_w_submitT() {
        return jobDesc_w_submitT;
    }

    public List<Put> convertToPut(){
        LinkedList<Put> puts = new LinkedList<Put>();


        JobKey jobKey_w_finishT = new JobKey(jobDesc_w_finishT);
        JobKey jobKey_w_submitT = new JobKey(jobDesc_w_submitT);


        this.jobKey = jobKey_w_submitT;
        this.jobKeyByTS = jobKeyConv.toBytesSortByTS(jobKey_w_finishT);
        this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
        setJobId(this.jobKey.getJobId().getJobIdString());

//        puts.addAll(getPut());
        puts.addAll(getJobPuts());
        puts.addAll(getJobStatusPuts());
        puts.addAll(getSU());



        return puts;
    }




    private void setJobId(String id) {
        if (id != null && id.startsWith("job_") && id.length() > 4) {
            this.jobNumber = id.substring(4);
        }
    }

    private List<Put> getPut(){
        return ExtendJobHistoryService.getHbasePuts(jobDesc_w_submitT, jobDesc_w_finishT, null);
    }

    private List<Put> getSU(){
        List<Put> ps = new LinkedList<Put>();
        long wall_clock_time = StartTimestamp - EndTimestamp;
        long cpu_hour = wall_clock_time * (long)ExecutorCount;

        Put pMb = new Put(this.jobKeyBytes);
        pMb.add(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(cpu_hour));
        ps.add(pMb);
        pMb = new Put(this.jobKeyByTS);
        pMb.add(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(cpu_hour));
        ps.add(pMb);

        return  ps;
    }

    private List<Put> getJobStatusPuts(){
        List<Put> lp = new LinkedList<Put>();
        Put pStatus;
        pStatus= new Put(jobKeyBytes);
        byte[] valueBytes = Bytes.toBytes(this.jobStatus);
        byte[] qualifier = Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase());
        pStatus.add(ExtendConstants.INFO_FAM_BYTES, qualifier, valueBytes);
        lp.add(pStatus);
        pStatus= new Put(jobKeyByTS);
        valueBytes = Bytes.toBytes(this.jobStatus);
        qualifier = Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase());
        pStatus.add(ExtendConstants.INFO_FAM_BYTES, qualifier, valueBytes);
        lp.add(pStatus);
        return lp;
    }

    private List<Put> getJobPuts(){
        List lp = new LinkedList<Put>();

        Put pJob = new Put(this.jobKeyBytes);
        populatePut(pJob, ExtendConstants.INFO_FAM_BYTES);
        lp.add(pJob);
        LOG.debug("add to Put list | RK = [ "+ Bytes.toStringBinary(jobKeyBytes)+" ]");

        // for scan Job sort by TS
        Put pJobT = new Put(this.jobKeyByTS);
        populatePut(pJobT, ExtendConstants.INFO_FAM_BYTES);
        lp.add(pJobT);
        LOG.debug("add to Put list | RK = [ "+ Bytes.toStringBinary(jobKeyBytes)+" ]");
        return lp;
    }

    private void populatePut(Put p, byte[] family){
        byte[] valueBytes;
        byte[] qualifier;

        valueBytes = Bytes.toBytes(StartTimestamp);
        qualifier = Bytes.toBytes("startTime");
        p.add(family, qualifier, valueBytes);

        valueBytes = Bytes.toBytes(EndTimestamp);
        qualifier = Bytes.toBytes("finishTime");
        p.add(family, qualifier, valueBytes);

        valueBytes = Bytes.toBytes(User);
        qualifier = Bytes.toBytes("userName");
        p.add(family, qualifier, valueBytes);

        valueBytes = Bytes.toBytes(AppName);
        qualifier = Bytes.toBytes("jobName");
        p.add(family, qualifier, valueBytes);

        valueBytes = Bytes.toBytes(ExecutorCount);
        qualifier = Bytes.toBytes("executorCount");
        p.add(family, qualifier, valueBytes);

        valueBytes = Bytes.toBytes(jobKey.getJobId().getJobIdString());
        qualifier = Bytes.toBytes("jobid");
        p.add(family, qualifier, valueBytes);
    }

    public void generateJobDesc(QualifiedJobId qualifiedJobId){
        if(jobDesc_w_finishT == null)
            jobDesc_w_finishT = new JobDesc(qualifiedJobId, User, AppName , Constants.UNKNOWN,EndTimestamp, Framework.NONE);

        if(jobDesc_w_submitT == null)
            jobDesc_w_submitT = new JobDesc(qualifiedJobId, User, AppName, Constants.UNKNOWN, StartTimestamp, Framework.NONE);
    }

    @Override
    public String toString() {
        return "SparkDetails{" +
                "AppName='" + AppName + '\'' +
                ", AppID='" + AppID + '\'' +
                ", StartTimestamp=" + StartTimestamp +
                ", EndTimestamp=" + EndTimestamp +
                ", ExecutorCount=" + ExecutorCount +
                ", User='" + User + '\'' +
                '}';
    }
}
