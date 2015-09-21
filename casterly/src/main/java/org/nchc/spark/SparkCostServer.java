package org.nchc.spark;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.RowKeyParseException;
import com.twitter.hraven.etl.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.nchc.extend.ExtendConstants;
import org.nchc.extend.ExtendJobHistoryByIdService;
import org.nchc.extend.ExtendJobHistoryRawService;
import org.nchc.history.PutUtil;
import org.nchc.spark.dao.SparkDetails;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by 1403035 on 2015/5/5.
 */
public class SparkCostServer extends Thread {
    private static Log LOG = LogFactory.getLog(SparkCostServer.class);
    private boolean isRunning = true;
    private String cluster = "nchc";
    private String appType = "spark";
    private static int INTERVAL = 10000;
    private Configuration hbaseConf;
    private FileSystem hdfs;
    private static Path inputPath = new Path("/user/hdadm/spark_log/applicationHistory");
    private final static long DEFAULT_RAW_FILE_SIZE_LIMIT = 524288000;
    private HashMap<JobFile, FileStatus> jobstatusmap = null;

    private HTable rawTable = null;
    private HTable jobTable = null;

    private ProcessRecordService processRecordService = null;
    private ExtendJobHistoryRawService rawService = null;
    private ExtendJobHistoryByIdService jobHistoryByIdService = null;

    public SparkCostServer(Properties ps) throws IOException {
        initTable();

    }

    public static void main(String[] args) throws IOException, RowKeyParseException {
        SparkCostServer sc = new SparkCostServer(null);
        sc.run();
    }


    private static void printResult(Result r){
        StringBuilder sb = new StringBuilder();
        sb.append("ROWKEY=").append(Bytes.toStringBinary(r.getRow())).append("\n").append("KV=[");
        for (KeyValue kv : r.raw()) {
            sb.append(Bytes.toString(kv.getFamily())).append(":").
                    append(Bytes.toString(kv.getQualifier())).append("=")
                    .append(Bytes.toString(kv.getValue())).append(",");

        }
        sb.append("]").append("\n");
        LOG.info(sb.toString());
    }

    private void initTable() throws IOException {
        hbaseConf = new Configuration();
        hbaseConf.setStrings(Constants.CLUSTER_JOB_CONF_KEY, cluster);
        hdfs = FileSystem.get(hbaseConf);
        processRecordService = new ProcessRecordService(hbaseConf);

        rawTable = new HTable(hbaseConf, Constants.HISTORY_RAW_TABLE_BYTES);
        jobTable = new HTable(hbaseConf, Constants.HISTORY_TABLE_BYTES);

        jobstatusmap = new HashMap<JobFile, FileStatus>();
        rawService = new ExtendJobHistoryRawService(hbaseConf);
        jobHistoryByIdService = new ExtendJobHistoryByIdService(hbaseConf);
    }

    @Override
    public void run() {
        LOG.info("start separate thread to parse SPARK file");
        while (isRunning) {
            try {

                // 1. find out files
                FileStatus[] jobFileStatusses = findProcessFiles();
                if (jobFileStatusses.length < 1) {
                    Thread.sleep(INTERVAL);
                    continue;
                }

                // 2. put spark raw data to hbase
                MinMaxJobFileTracker filetracker = loadRAW(jobFileStatusses);

                //3. hadnle file retrived from HBase
                //  3.1 modify spark process table
                //  3.2 delete processed spark history file
                loadSparkDetail(cluster,filetracker.getMinJobId(),filetracker.getMaxJobId());


                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                LOG.info("Spark parsing Thread was inturrupted");
            } catch (Exception e) {
                StringWriter errors = new StringWriter();
                e.printStackTrace(new PrintWriter(errors));
                LOG.error(errors.toString());
            }
        }
    }


    private void loadSparkDetail(String cluster, String minJobId, String maxJobId)
            throws IOException, RowKeyParseException {
        ResultScanner scanner = rawService.getHistoryRawTableScansWithoutRaw(cluster, minJobId, maxJobId);
        EventLogReader reader = new EventLogReader();

        for (Result result : scanner) {
            byte[] sparkpath = rawService.getSparkFilePathFromResult(result);
            SparkDetails details = reader.read(Bytes.toString(sparkpath));
            LOG.info(details.toString());


            Put finishTimePut = rawService.getJobFinishTimePut(result.getRow(), details.getEndTimestamp());
            rawTable.put(finishTimePut);




            QualifiedJobId qualifiedJobId  = rawService.getQualifiedJobIdFromResult(result);
            details.generateJobDesc(qualifiedJobId);

            LOG.info("Writing secondary indexes");
            jobHistoryByIdService.writeIndexes(new JobKey(details.getJobDesc_w_submitT()));

            List<Put> jobputs = details.convertToPut();

            jobTable.put(jobputs);

            // delete log file after put into HBase
            // reader.delete(Bytes.toString(sparkpath));
        }

    }

    private FileStatus[] findProcessFiles() throws IOException {
        long processingStartMillis = System.currentTimeMillis();

        // Figure out where we last left off (if anywhere at all)
        ProcessRecord lastProcessRecord;

        lastProcessRecord = processRecordService.getLastSuccessfulProcessRecord(cluster+appType);

        long minModificationTimeMillis = 0;
        if (lastProcessRecord != null) {
            // Start of this time period is the end of the last period.
            minModificationTimeMillis = lastProcessRecord.getMaxModificationTimeMillis();
        }

        // Do a sanity check. The end time of the last scan better not be later
        // than when we started processing.
        if (minModificationTimeMillis > processingStartMillis) {
            throw new RuntimeException(
                    "The last processing record has maxModificationMillis later than now: "
                            + lastProcessRecord);
        }

        // Accept only jobFiles and only those that fall in the desired range of
        // modification time.
        SparkFilePathFilter sparkFilePathFilter = new SparkFilePathFilter(hbaseConf, minModificationTimeMillis);

        String timestamp = Constants.TIMESTAMP_FORMAT.format(new Date(
                minModificationTimeMillis));

        ContentSummary contentSummary = hdfs.getContentSummary(inputPath);
        LOG.info("Listing / filtering ("
                + contentSummary.getFileCount() + ") files in: " + inputPath
                + " that are modified since " + timestamp);

        // get the files in the spark history folder,
        // need to traverse dirs under done recursively
        return FileLister.getListFilesToProcess(DEFAULT_RAW_FILE_SIZE_LIMIT, true,
                hdfs, inputPath, sparkFilePathFilter);
    }


    private MinMaxJobFileTracker loadRAW(FileStatus[] jobFileStatusses) throws IOException {
        LOG.debug("NEW history parse iteration....");
        LOG.info("Sorting " + jobFileStatusses.length + " job files.");
        Arrays.sort(jobFileStatusses, new FileStatusModificationComparator());
        MinMaxJobFileTracker minMaxJobFileTracker = new MinMaxJobFileTracker();
        ProcessRecord processRecord = processBatch(jobFileStatusses);
        minMaxJobFileTracker.track(processRecord.getMinJobId());
        minMaxJobFileTracker.track(processRecord.getMaxJobId());

        Iterator it = jobstatusmap.entrySet().iterator();
        List<Put> puts = new LinkedList<Put>();
        while (it.hasNext()) {
            Map.Entry<JobFile, FileStatus> pairs = (Map.Entry) it.next();
            loadRawHistory(pairs.getKey(), pairs.getValue(), puts);
        }
        rawTable.put(puts);
        rawTable.flushCommits();
        processRecordService.setProcessState(processRecord, ProcessState.LOADED);
        jobstatusmap.clear();

        return minMaxJobFileTracker;
    }

    private void loadRawHistory(JobFile jobFile, FileStatus fileStatus, List<Put> puts) throws IOException {
        boolean exists = hdfs.exists(fileStatus.getPath());
        if (exists) {
            if (jobFile.isSparkFile()) {
                byte[] rowKey = getRowKeyBytes(jobFile);
                // Add filename to be used to re-create JobHistory URL later
                PutUtil.addFilePathPut(puts, rowKey,
                        ExtendConstants.SPARK_FILEPATH_COL_BYTES,
                        Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES,
                        Constants.JOBHISTORY_FILENAME_COL_BYTES,
                        jobFile.getFilename(),
                        fileStatus);
                LOG.info("Loaded spark history file path: " + fileStatus.getPath());

            } else {
                LOG.warn("Skipping Key: " + jobFile.getFilename());
            }
        } else {
            LOG.warn("Unable to find file: " + fileStatus.getPath());
        }
    }

    private byte[] getRowKeyBytes(JobFile jobFile) {
        String cluster = hbaseConf.get(Constants.CLUSTER_JOB_CONF_KEY);
        return rawService.getRowKey(cluster, jobFile.getJobid());
    }

    private ProcessRecord processBatch(FileStatus[] jobFileStatusses) throws IOException {
        MinMaxJobFileTracker minMaxJobFileTracker = new MinMaxJobFileTracker();

        for (int i = 0; i < jobFileStatusses.length; i++) {
            jobstatusmap.put(minMaxJobFileTracker.trackSpark(jobFileStatusses[i]), jobFileStatusses[i]);
        }
        ProcessRecord processRecord = new ProcessRecord(cluster+appType, ProcessState.PREPROCESSED,
                minMaxJobFileTracker.getMinModificationTimeMillis(),
                minMaxJobFileTracker.getMaxModificationTimeMillis(),
                jobFileStatusses.length,
                "N/A", minMaxJobFileTracker.getMinJobId(),
                minMaxJobFileTracker.getMaxJobId());

        LOG.info("Creating processRecord: " + processRecord);

        processRecordService.writeJobRecord(processRecord);

        return processRecord;
    }


    public void stopThread() {
        isRunning = false;
    }
}
