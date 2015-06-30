package org.nchc.history;

import com.twitter.hraven.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.nchc.extend.ExtendConstants;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

/**
 * Created by 1403035 on 2015/1/8.
 */
public class Casterly {
    private static Log LOG = LogFactory.getLog(Casterly.class);
    private static String PROPFILENAME = "casterly.properties";

    public static void main(String[] args) throws Exception {
        new Casterly().run(args);
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
        task.start();

        LOG.info("START parse history");
        JobCostServer jcs = new JobCostServer(ps);
        jcs.getArgValue(args);
        jcs.start();

        addShutdownHook(task,server,jcs);
    }

    private void addShutdownHook(final RunningJobID task,
                                        final RestServer server,
                                        final JobCostServer jcs) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info("STOP scan running job thread...");
                task.stopThread();
                task.interrupt();

                try {
                    LOG.info("STOP embbed Jetty Server");
                    server.shutDown();
                } catch (Exception e) {
                    StringWriter errors = new StringWriter();
                    e.printStackTrace(new PrintWriter(errors));
                    LOG.error(errors.toString());
                }

                LOG.info("STOP JobCostServer thread");
                jcs.stopThread();
                jcs.interrupt();
            }
        });
    }


    private Properties init(String propname){
        Configuration conf;
        HBaseAdmin hBaseAdmin;

        Properties ps = PutUtil.loadCostProperties(propname);
        if(ps == null){
            LOG.error("Can not read " + PROPFILENAME);
            return null;
        }

        // check history.hdfsdir in HDFS
        if(!ps.containsKey("history.hdfsdir") ||
                !checkHDFS(ps.getProperty("history.hdfsdir"))){
            LOG.error("check HDFS configuration");
            return null;
        }

        // check yarn.RM_web
        if(!ps.containsKey("running.yarn.RM_web") ||
                !checkConnected(ps.getProperty("running.yarn.RM_web"))) {
            LOG.error("check yarn RM restful server configuration");
           return null;
        }
        // check PROXY_web
//        if(!ps.containsKey("running.yarn.PROXY_web") ||
//                !checkConnected(ps.getProperty("running.yarn.PROXY_web"))) {
//            LOG.error("check yarn Proxy restful server configuration");
//            return null;
//        }

        conf = HBaseConfiguration.create();

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
            LOG.error("Can not connect to zookeeper ");
            ps = null;
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOG.error(errors.toString());
        }
        return ps;
    }


    private boolean checkHDFS(String path){
        Configuration conf = HBaseConfiguration.create();
        FileSystem hdfs;
        try {
             hdfs = FileSystem.get(conf);
            return hdfs.exists(new Path(path))? true:false;
        } catch (IOException e) {
            LOG.error("Can not access HDFS");
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOG.error(errors.toString());
            return false;
        }
    }


    private boolean checkConnected(String host) {
        URL url;
        HttpURLConnection urlc = null;
        try {
            // Network is available but check if we can get access from the network
            url = new URL(host);
            urlc = (HttpURLConnection) url.openConnection();
            urlc.setRequestProperty("Connection", "close");
            urlc.setConnectTimeout(2000); // Timeout 2 seconds.
            urlc.connect();

            if (urlc.getResponseCode() == 200){
                return true;
            } else {
                LOG.warn("NO INTERNET");
                return false;
            }
        } catch (Exception e) {
            LOG.error("NO connection ");
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOG.error(errors.toString());
        }finally {
            if(urlc != null){
                urlc.disconnect();
            }
        }
        return false;
    }

}