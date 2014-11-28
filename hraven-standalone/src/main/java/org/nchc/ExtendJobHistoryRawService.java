package org.nchc;

import com.twitter.hraven.datasource.JobHistoryRawService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

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
}
