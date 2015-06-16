package org.nchc.extend;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobId;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.JobHistoryRawService;
import com.twitter.hraven.datasource.MissingColumnInResultException;
import com.twitter.hraven.datasource.QualifiedJobIdConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobHistoryRawService extends JobHistoryRawService{

    private static Log LOG = LogFactory.getLog(ExtendJobHistoryRawService.class);
    private QualifiedJobIdConverter idConv = new QualifiedJobIdConverter();

    public ExtendJobHistoryRawService(Configuration myHBaseConf) throws IOException {
        super(myHBaseConf);
    }

    public ResultScanner getHistoryRawTableScans(String cluster, String minJobId, String maxJobId) throws IOException {
        ResultScanner scanner = null;
        Scan scan = getHistoryRawTableScan(cluster, minJobId, maxJobId, false, true);
        scanner = rawTable.getScanner(scan);
        return scanner;
    }

    public ResultScanner getHistoryRawTableScansWithoutRaw(String cluster, String minJobId, String maxJobId) throws IOException {
        ResultScanner scanner = null;
        Scan scan = getSparrkHistoryRawTableScan(cluster, minJobId, maxJobId, false, false);
        scanner = rawTable.getScanner(scan);
        return scanner;
    }

    /**
     * identical to getHistoryRawTableScan(),
     * except a filter that passes only if both the jobconf and job history
     * blobs are present.
     **/
    private Scan getSparrkHistoryRawTableScan(String cluster, String minJobId, String maxJobId,
                                              boolean reprocess, boolean includeRaw){

        Scan scan = new Scan();

        LOG.info("Creating scan for cluster: " + cluster);

        // Add the columns to be pulled back by this scan.
        scan.addFamily(Constants.INFO_FAM_BYTES);
        if (includeRaw) {
            scan.addFamily(Constants.RAW_FAM_BYTES);
        }

        // Pull data only for our cluster
        byte[] clusterPrefix = Bytes.toBytes(cluster + Constants.SEP);
        byte[] startRow;
        if (minJobId == null) {
            startRow = clusterPrefix;
        } else {
            startRow = idConv.toBytes(new QualifiedJobId(cluster, minJobId));
        }
        scan.setStartRow(startRow);

        LOG.info("Starting raw table scan at " + Bytes.toStringBinary(startRow) + " " + idConv.fromBytes(startRow));

        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // Scan only those raw rows for the specified cluster.
        PrefixFilter prefixFilter = new PrefixFilter(clusterPrefix);
        filters.addFilter(prefixFilter);

        byte[] stopRow;
        // Set to stop the scan once the last row is encountered.
        if (maxJobId != null) {
            // The inclusive stop filter actually is the accurate representation of
            // what needs to be in the result.
            byte[] lastRow = idConv.toBytes(new QualifiedJobId(cluster, maxJobId));
            InclusiveStopFilter inclusiveStopFilter = new InclusiveStopFilter(lastRow);
            filters.addFilter(inclusiveStopFilter);
            LOG.info("Stopping raw table scan (stop filter) at "
                    + Bytes.toStringBinary(lastRow) + " " + idConv.fromBytes(lastRow));

            // Add one to the jobSequence of the maximum JobId.
            JobId maximumJobId = new JobId(maxJobId);
            JobId oneBiggerThanMaxJobId = new JobId(maximumJobId.getJobEpoch(),
                    maximumJobId.getJobSequence() + 1);
            stopRow = idConv.toBytes(new QualifiedJobId(cluster,
                    oneBiggerThanMaxJobId));

        } else {
            char oneBiggerSep = (char) (Constants.SEP_CHAR + 1);
            stopRow = Bytes.toBytes(cluster + oneBiggerSep);
        }
        // In addition to InclusiveStopRowFilter, set an estimated end-row that is
        // guaranteed to be bigger than the last row we want (but may over-shoot a
        // little). This helps the TableInput format limit the number of regions
        // (-servers) that need to be targeted for this scan.
        scan.setStopRow(stopRow);
        LOG.info("Stopping raw table scan (stop row) at "
                + Bytes.toStringBinary(stopRow));

        scan.setFilter(filters);

        if (reprocess) {
            SingleColumnValueExcludeFilter columnValueFilter = new SingleColumnValueExcludeFilter(
                    Constants.INFO_FAM_BYTES, Constants.RAW_COL_REPROCESS_BYTES,
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes(true));
            columnValueFilter.setFilterIfMissing(true);
            filters.addFilter(columnValueFilter);
        } else {
            // Process each row only once. If it is already processed, then do not do
            // it again.
            SingleColumnValueExcludeFilter columnValueFilter = new SingleColumnValueExcludeFilter(
                    Constants.INFO_FAM_BYTES, Constants.JOB_PROCESSED_SUCCESS_COL_BYTES,
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true));
            filters.addFilter(columnValueFilter);
        }
        return scan;
    }

    public Put getJobFinishTimePut(byte[] row, long finishTimeMillis) {
        Put put = new Put(row);
        put.add(Constants.INFO_FAM_BYTES, ExtendConstants.FINISH_TIME_COL_BYTES,
                Bytes.toBytes(finishTimeMillis));
        return put;
    }

    public long getApproxFinishTime(Result value) throws MissingColumnInResultException {
        if (value == null) {
            throw new IllegalArgumentException("Cannot get last modification time from "
                    + "a null hbase result");
        }

        KeyValue keyValue = value.getColumnLatest(Constants.INFO_FAM_BYTES,
                Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES);

        if (keyValue == null) {
            throw new MissingColumnInResultException(Constants.INFO_FAM_BYTES,
                    Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES);
        }

        byte[] lastModTimeBytes = keyValue.getValue();
        // we try to approximately set the job finish time was the last modified
        long lastModTime = Bytes.toLong(lastModTimeBytes);
        return lastModTime;
    }

    public byte[] getSparkFilePathFromResult(Result result){
//        KeyValue kv = result.getColumnLatest(Constants.INFO_FAM_BYTES, ExtendConstants.SPARK_FILEPATH_COL_BYTES);
        byte[] value = result.getValue(Constants.INFO_FAM_BYTES,ExtendConstants.SPARK_FILEPATH_COL_BYTES);
//        byte[] value = kv.getValue();
        return value;
    }
}
