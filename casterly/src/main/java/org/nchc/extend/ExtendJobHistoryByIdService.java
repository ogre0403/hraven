package org.nchc.extend;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.JobHistoryByIdService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobHistoryByIdService extends JobHistoryByIdService {

    public ExtendJobHistoryByIdService(Configuration myHBaseConf) throws IOException {
        super(myHBaseConf);
    }

    public void writeIndexes(JobKey jobKey) throws IOException {
        // Defensive coding
        if (jobKey != null) {
            byte[] jobKeyBytes = jobKeyConv.toBytes(jobKey);
            byte[] rowKeyBytes = jobIdConv.toBytes(
                    new QualifiedJobId(jobKey.getCluster(), jobKey.getJobId()) );

            // Insert (or update) row with jobid as the key
            Put p = new Put(rowKeyBytes);
            p.add(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES, jobKeyBytes);
            historyByJobIdTable.put(p);
        }
    }
}
