package org.nchc.history;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.util.ByteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 1403035 on 2014/11/12.
 */
public class TestRealData {

    private static final Configuration HBASE_CONF = new Configuration();

    @Ignore
    public void testJobHistoryRead() throws IOException {
        HBASE_CONF.set("hbase.zookeeper.quorum","192.168.56.201");
        JobHistoryService jhs = new JobHistoryService(HBASE_CONF);
        JobDetails jobDetails = jhs.getJobByJobID("NCHC", "job_1415006048688_000007");
        System.out.println(jobDetails.getQueue());
        Configuration cc = jobDetails.getConfiguration();
        System.out.println("aa");
    }

    @Ignore
    public void testfromTsSortedBytes() throws IOException {
        HBASE_CONF.set("hbase.zookeeper.quorum","192.168.56.201");
        queryJobService qser = new queryJobService(HBASE_CONF);
        List<JobDetails> jj = qser.getAllJobInTimeInterval("NCHC","hdadm");
        qser.close();
    }

    @Test
    public void testAllJobRK() throws IOException {
        HBASE_CONF.set("hbase.zookeeper.quorum","192.168.56.201");
        byte[] tmprow = Bytes.add(Bytes.toBytes("NCHC"), Constants.SEP_BYTES,Bytes.toBytes("hdadm"));
        byte[] row = Bytes.add(tmprow,Constants.SEP3_BYTES,Bytes.toBytes("TeraGen"));
        Get g = new Get(row);

        HTable table = new HTable(HBASE_CONF,"job_history");
        Result r = table.get(g);
        if(!r.isEmpty()){
        Iterator<byte[]> iter = r.getFamilyMap(Constants.INFO_FAM_BYTES).navigableKeySet().iterator();
        while(iter.hasNext()){
            System.out.println(Bytes.toString(iter.next()));
        }
        }
        table.close();
    }

    @Ignore
    public void testAllJobRK2() throws IOException {
        HBASE_CONF.set("hbase.zookeeper.quorum","192.168.56.201");
        HTable table = new HTable(HBASE_CONF,"job_history");
        byte[] tmprow = Bytes.add(Bytes.toBytes("NCHC"), Constants.SEP_BYTES,Bytes.toBytes("hdadm"));
        byte[] start = Bytes.add(tmprow,Constants.SEP3_BYTES);
        byte[] end = Bytes.add(tmprow,Constants.SEP4_BYTES);
        Scan scan = new Scan();
        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner rs  = table.getScanner(scan);
        for(Result r:rs){
            byte[] row = r.getRow();
            //byte[][] ff = ByteUtil.split(row,Constants.SEP3_BYTES);
            //System.out.println(Bytes.toString(ff[1]));
            System.out.println(Bytes.toString(ByteUtil.split(row,Constants.SEP3_BYTES)[1]));
        }
        table.close();
    }

}
