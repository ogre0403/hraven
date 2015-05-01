package org.nchc.extend;

import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.JobHistoryRawService;
import com.twitter.hraven.datasource.MissingColumnInResultException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobHistoryRawService extends JobHistoryRawService{

    public ExtendJobHistoryRawService(Configuration myHBaseConf) throws IOException {
        super(myHBaseConf);
    }

    public ResultScanner getHistoryRawTableScans(String cluster, String minJobId, String maxJobId) throws IOException {
        ResultScanner scanner = null;
        Scan scan = getHistoryRawTableScan(cluster, minJobId, maxJobId, false, true);
        scanner = rawTable.getScanner(scan);
        return scanner;
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
}
