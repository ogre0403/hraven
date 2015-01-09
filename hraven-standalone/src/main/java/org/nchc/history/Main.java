package org.nchc.history;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import java.util.Properties;

/**
 * Created by 1403035 on 2015/1/8.
 */
public class Main {
    private static Log LOG = LogFactory.getLog(Main.class);

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception{

        init();
        Properties ps = PutUtil.loadCostProperties("cost.properties");

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

    public void close(){
        //TODO: close HTable and connections gracefully
    }

    public void init(){
        //TODO: check Htable existence
    }


}
