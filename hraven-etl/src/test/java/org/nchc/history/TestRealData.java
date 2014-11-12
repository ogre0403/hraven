package org.nchc.history;

import com.twitter.hraven.JobDetails;
import com.twitter.hraven.datasource.JobHistoryService;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/11/12.
 */
public class TestRealData {

    private static final Configuration HBASE_CONF = new Configuration();

    @Test
    public void testJobHistoryRead() throws IOException {
        HBASE_CONF.set("hbase.zookeeper.quorum","192.168.56.201");
        JobHistoryService jhs = new JobHistoryService(HBASE_CONF);
        JobDetails jobDetails = jhs.getJobByJobID("NCHC", "job_1415006048688_000007");
        System.out.println(jobDetails.getQueue());
        Configuration cc = jobDetails.getConfiguration();


        System.out.println("aa");
    }
}
