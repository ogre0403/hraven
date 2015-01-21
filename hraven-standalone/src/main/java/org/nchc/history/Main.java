package org.nchc.history;

import com.twitter.hraven.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.nchc.extend.ExtendConstants;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

/**
 * Created by 1403035 on 2015/1/8.
 */
public class Main {
    private static Log LOG = LogFactory.getLog(Main.class);
    private static String PROPFILENAME = "cost.properties";

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception{

        Properties ps = init(PROPFILENAME);
        if(ps == null){
            LOG.error("initial fail, STOP");
            return;
        }

        LOG.info("START embedded jetty server...");
        RestServer server = new RestServer(ps);
        server.startUp();

        LOG.info("START scan running job thread...");
        RunningJobID task = new RunningJobID(ps);
        Thread t = new Thread(task);
        t.start();

        LOG.info("START parse history");
        ToolRunner.run(new JobCostServer(ps), args);
    }


    public Properties init(String propname){
        Configuration conf;
        HBaseAdmin hBaseAdmin;

        Properties ps = PutUtil.loadCostProperties(propname);
        if(ps == null){
            LOG.error("Can not read " + PROPFILENAME);
            return null;
        }

        conf = new Configuration();
        String ZK = ps.getProperty("zookeeper","127.0.0.1");
        conf.set("hbase.zookeeper.quorum",ZK);

        //check Htable existence
        try {
            hBaseAdmin = new HBaseAdmin(conf);
            if( !hBaseAdmin.tableExists(Constants.HISTORY_TABLE) ||
                !hBaseAdmin.tableExists(Constants.HISTORY_TASK_TABLE) ||
                !hBaseAdmin.tableExists(Constants.HISTORY_BY_JOBID_TABLE) ||
                !hBaseAdmin.tableExists(Constants.HISTORY_RAW_TABLE) ||
                !hBaseAdmin.tableExists(Constants.JOB_FILE_PROCESS_TABLE)||
                !hBaseAdmin.tableExists(ExtendConstants.RUNNING_JOB_TABLE) ){
                LOG.error("Table not exist");
                return null;
            }
        } catch (IOException e) {
            LOG.error("Can not connect to zookeeper " + ZK);
            ps = null;
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOG.error(errors.toString());
        }
        return ps;
    }


    public void close(){
        //TODO: close HTable and connections gracefully
    }



}
