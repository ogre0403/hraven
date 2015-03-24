package org.nchc.history;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.nchc.extend.ExtendConstants;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by 1403035 on 2015/1/5.
 */
public class RunningJobID extends Thread {

    private static Log LOG = LogFactory.getLog(RunningJobID.class);
    private boolean isRunning = true;

    private static String jobs_url;
    private static JsonParser parser = new JsonParser();
    private static DefaultHttpClient httpClient = new DefaultHttpClient();
    private static int INTERVAL = 30000;
    private static HTable htable;
    private static Configuration conf = null;

//  private static String ZK = "127.0.0.1";
//  private static String STATE = "RUNNING";
//  private static String RUNNING_TABLE = "job_running";

    public RunningJobID(Properties ps) {

        conf = HBaseConfiguration.create();
        //http://192.168.56.201:8088/ws/v1/cluster/apps/?state=RUNNING
        //jobs_url = ps.getProperty("running.yarn_rest","http://127.0.0.1:8088/ws/v1/cluster/apps/");
        String restserver = ps.getProperty("running.yarn.RM_web","http://192.168.56.201:8088");
        jobs_url = restserver+"/ws/v1/cluster/apps/?state=RUNNING";
        INTERVAL = Integer.parseInt(ps.getProperty("running.interval"));
        LOG.info("running.yarn.restserver: " + restserver);
        LOG.info("running.yarn.resturl: " + jobs_url);
        LOG.info("running.interval: " + INTERVAL);

        try {
            htable = new HTable(conf,ExtendConstants.RUNNING_JOB_TABLE);
            htable.setAutoFlush(false);
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOG.error(errors.toString());
        }
    }

    @Override
    public void run() {

        while(isRunning){
            try
            {
                Map<String, Map<String,StringBuilder>> jobs = getJobID2();
                if (jobs == null){
                    Thread.sleep(INTERVAL);
                    continue;
                }
                LOG.info("put running job into HBase");
                List<Put> lp = toPuts(jobs);
                htable.put(lp);
                htable.flushCommits();
                Thread.sleep(INTERVAL);
            }
            catch(InterruptedException e)
            {
                LOG.info("Thread was inturrupted");
            }
            catch(Exception e) {
                StringWriter errors = new StringWriter();
                e.printStackTrace(new PrintWriter(errors));
                LOG.error(errors.toString());
            }
        }
    }

    private List<Put> toPuts(Map<String, Map<String,StringBuilder>> data){
        List<Put> lp = new LinkedList<Put>();

        Iterator iter = data.entrySet().iterator();
        while (iter.hasNext()) {

            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            Map<String,StringBuilder> val = (Map<String,StringBuilder>)entry.getValue();
            LOG.debug("running job user is :" + key);
            Put p = new Put(Bytes.toBytes(key));
            Iterator inner_iter = val.entrySet().iterator();
            while(inner_iter.hasNext()){
                Map.Entry inner_entry = (Map.Entry) inner_iter.next();
                String inner_key = (String) inner_entry.getKey();
                StringBuilder inner_val = (StringBuilder)inner_entry.getValue();
                p.add(Bytes.toBytes("r"),Bytes.toBytes(inner_key),Bytes.toBytes(inner_val.toString()));
            }
            lp.add(p);
        }

        return lp;
    }

    private Map<String, Map<String,StringBuilder>> getJobID2() throws IOException {
        Map<String, Map<String,StringBuilder>> tmpdata = new HashMap<String, Map<String,StringBuilder>>();

        HttpGet getRequest = new HttpGet(jobs_url);
        getRequest.addHeader("accept", "application/json");
        HttpResponse response = httpClient.execute(getRequest);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }
        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        JsonObject joo = ee.getAsJsonObject();
        if (joo.get("apps").isJsonNull()){
            return null;
        }
        JsonArray jaa = joo.getAsJsonObject("apps").getAsJsonArray("app");


        for(final JsonElement type : jaa) {
            JsonObject coords = type.getAsJsonObject();
          //  if(coords.getAsJsonPrimitive("state").getAsString().equals(STATE)){
                String username = coords.getAsJsonPrimitive("user").getAsString();
                String jobname = coords.getAsJsonPrimitive("name").getAsString();
                String jobid = coords.getAsJsonPrimitive("id").getAsString();

                if(!tmpdata.containsKey(username)){
                    Map<String, StringBuilder> m2 = new HashMap<String, StringBuilder>();
                    StringBuilder sb = new StringBuilder();
                    sb.append(jobid);
                    m2.put(jobname,sb);
                    tmpdata.put(username,m2);
                }else{
                    Map<String, StringBuilder> m2 = tmpdata.get(username);
                    if(!m2.containsKey(jobname)){
                        StringBuilder sb = new StringBuilder();
                        sb.append(jobid);
                        m2.put(jobname,sb);
                    }else{
                        StringBuilder sb = m2.get(jobname);
                        sb.append(ExtendConstants.SEP5).append(jobid);
                    }
                }
           // }
        }
        return tmpdata;
    }


    public void stopThread(){
        try {
            LOG.info("stop HTable: job_running ");
            htable.close();
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOG.error(errors.toString());
        }
        httpClient.getConnectionManager().shutdown();
        LOG.info("stop http client");
        this.isRunning = false;
    }

}
