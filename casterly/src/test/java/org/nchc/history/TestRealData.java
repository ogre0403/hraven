package org.nchc.history;

//import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.util.ByteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.nchc.extend.ExtendConstants;
import org.nchc.rest.QueryJobService;
import org.nchc.rest.RunningStatusDAO;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by 1403035 on 2014/11/12.
 */
public class TestRealData {

    private static final Configuration HBASE_CONF = new Configuration();
    private static HTable table;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        HBASE_CONF.set("hbase.zookeeper.quorum","192.168.56.201");
        table = new HTable(HBASE_CONF,"job_history");
//        table = new HTable(HBASE_CONF,"test2");
    }

    @Ignore
    public void testJobHistoryRead() throws IOException {

        JobHistoryService jhs = new JobHistoryService(HBASE_CONF);
        JobDetails jobDetails = jhs.getJobByJobID("NCHC", "job_1415006048688_000007");
        System.out.println(jobDetails.getQueue());
        Configuration cc = jobDetails.getConfiguration();
        System.out.println("aa");
    }

    @Ignore
    public void testfromTsSortedBytes() throws IOException {
        QueryJobService qser = new QueryJobService(HBASE_CONF);
        List<JobDetails> jj = qser.getAllJobInTimeInterval("NCHC","hdadm",false);
        qser.close();
    }

    @Ignore
    public void testAllJobRK() throws IOException {
        byte[] tmprow = Bytes.add(Bytes.toBytes("NCHC"), ExtendConstants.SEP_BYTES,Bytes.toBytes("hdadm"));
        byte[] row = Bytes.add(tmprow,ExtendConstants.SEP3_BYTES,Bytes.toBytes("TeraGen"));
        Get g = new Get(row);
        Result r = table.get(g);
        if(!r.isEmpty()){
        Iterator<byte[]> iter = r.getFamilyMap(ExtendConstants.INFO_FAM_BYTES).navigableKeySet().iterator();
            while(iter.hasNext()){
                System.out.println(Bytes.toString(iter.next()));
            }
        }
    }

    @Ignore
    public void testAllJobRK2() throws IOException {
        byte[] tmprow = Bytes.add(Bytes.toBytes("NCHC"), ExtendConstants.SEP_BYTES,Bytes.toBytes("hdadm"));
        byte[] start = Bytes.add(tmprow,ExtendConstants.SEP3_BYTES);
        byte[] end = Bytes.add(tmprow,ExtendConstants.SEP4_BYTES);
        Scan scan = new Scan();
        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner rs  = table.getScanner(scan);
        for(Result r:rs){
            byte[] row = r.getRow();
            //byte[][] ff = ByteUtil.split(row,Constants.SEP3_BYTES);
            //System.out.println(Bytes.toString(ff[1]));
            System.out.println(Bytes.toString(ByteUtil.split(row,ExtendConstants.SEP3_BYTES)[1]));
        }
    }

    @Ignore
    public void testFilter() throws IOException{
        byte[] start = ByteUtil.join(ExtendConstants.SEP_BYTES,Bytes.toBytes("NCHC"),Bytes.toBytes("hdadm"),Bytes.toBytes("TeraGen"));
        byte[] end = ByteUtil.join(ExtendConstants.SEP_BYTES,Bytes.toBytes("NCHC"),Bytes.toBytes("hdadm"),Bytes.toBytes("U"));

        Scan s  =new Scan();
        s.setStartRow(start);
        s.setStopRow(end);
        Filter qfilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL,
                new RegexStringComparator("^(c!|g!|gm!|gr!)"));
        s.setFilter(qfilter);
        ResultScanner rs  = table.getScanner(s);
        NavigableMap<byte[], byte[]>  mm;
        for(Result r:rs){
            if(!r.isEmpty()){
                mm = r.getFamilyMap(ExtendConstants.INFO_FAM_BYTES);
                Iterator<byte[]> iter = mm.navigableKeySet().iterator();
                while(iter.hasNext()){
                    byte[] kk = iter.next();
                    byte[] vv = mm.get(kk);
                    System.out.println(Bytes.toString(kk)+" = " + Bytes.toString(vv));
                }
            }
        }
    }

    @Ignore
    public void testHttpServer() throws Exception {
        RestServer server = new RestServer("0.0.0.0", 8080);
        server.startUp();
//        Thread.sleep(100000000);
    }

    @Ignore
    public void testQueryRunningJob() throws Exception{
        QueryJobService qq = new QueryJobService(HBASE_CONF);
        List<String> list = qq.getRunningJobName("hdadm");
        for(String s : list){
            System.out.println(s);
        }
    }

    @Ignore
    public void testQueryRunningJobID() throws Exception{
        QueryJobService qq = new QueryJobService(HBASE_CONF);
        List<String> list = qq.getRunningJobID("hdadm","Read Image");
        for(String s : list){
            System.out.println(s);
        }
    }

    @Test
    public void testQueryRunningStatus() throws Exception{
        QueryJobService qq = new QueryJobService(HBASE_CONF);
        RunningStatusDAO rs = qq.getRunningJobStatus("application_1419570118547_0010");
        System.out.println(rs);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        table.close();
    }
}
