package org.nchc.history;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.nchc.extend.ExtendConstants;
import scala.collection.mutable.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.StringBuilder;
import java.util.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by 1403035 on 2014/12/26.
 */
public class MyHttpClient {

    private static String jobs_url = "http://192.168.56.201:8088/ws/v1/cluster/apps/";
    private static String progress_url_prefix = "http://192.168.56.201:8088/proxy/";
    private static String progress_url_postfix = "/ws/v1/mapreduce/jobs";
    private static JsonParser parser = new JsonParser();
    private static DefaultHttpClient httpClient = new DefaultHttpClient();
    private static String STATE = "RUNNING";

    private static String JSON_STR = "{\"jobs\":{\"job\":[{\"startTime\":1419993360735,\"finishTime\":0,\"elapsedTime\":380050,\"id\":\"job_1419570118547_0007\",\"name\":\"Read Image\",\"user\":\"hdadm\",\"state\":\"RUNNING\",\"mapsTotal\":84,\"mapsCompleted\":8,\"reducesTotal\":8,\"reducesCompleted\":0,\"mapProgress\":11.064249,\"reduceProgress\":0.0,\"mapsPending\":70,\"mapsRunning\":6,\"reducesPending\":8,\"reducesRunning\":0,\"uberized\":false,\"diagnostics\":\"\",\"newReduceAttempts\":8,\"runningReduceAttempts\":0,\"failedReduceAttempts\":0,\"killedReduceAttempts\":0,\"successfulReduceAttempts\":0,\"newMapAttempts\":70,\"runningMapAttempts\":6,\"failedMapAttempts\":0,\"killedMapAttempts\":0,\"successfulMapAttempts\":8}]}}";

    private static HTable htable;
    private static String ZK = "192.168.56.201";
    private static Configuration conf = null;

    public static void main(String[] args) throws IOException {
        /*
        Map<String, Map<String,StringBuilder>> jobs = getJobID2();
        List<Put> lp = toPuts(jobs);
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",ZK);
        htable = new HTable(conf,"job_running");
        htable.put(lp);
        htable.close();
        */



       // getRunningStatus("application_1419570118547_0007");
        httpClient.getConnectionManager().shutdown();
        parse_json();
    //    Map<String, List<String>> joblist =  getJobID();
    //    updateHBaseRuningJobTable(joblist);



    }

    private static List<Put> toPuts(Map<String, Map<String,StringBuilder>> data){
        List<Put> lp = new LinkedList<Put>();

        Iterator iter = data.entrySet().iterator();
        while (iter.hasNext()) {

            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            Map<String,StringBuilder> val = (Map<String,StringBuilder>)entry.getValue();

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



    private static Map<String, Map<String,StringBuilder>> getJobID2() throws IOException{
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
        JsonArray jaa = joo.getAsJsonObject("apps").getAsJsonArray("app");

        for(final JsonElement type : jaa) {
            JsonObject coords = type.getAsJsonObject();
            if(coords.getAsJsonPrimitive("state").getAsString().equals(STATE)){
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
            }
        }
        return tmpdata;
    }


    private static Map<String, List<String>> getJobID() throws IOException {

        Map<String, List<String>> lll = new HashMap<String, List<String>>();

        HttpGet getRequest = new HttpGet(jobs_url);
        getRequest.addHeader("accept", "application/json");
        HttpResponse response = httpClient.execute(getRequest);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }
        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));

        JsonObject joo = ee.getAsJsonObject();
        JsonArray jaa = joo.getAsJsonObject("apps").getAsJsonArray("app");


        for(final JsonElement type : jaa) {
            JsonObject coords = type.getAsJsonObject();
            if(coords.getAsJsonPrimitive("state").getAsString().equals(STATE)){
                String username = coords.getAsJsonPrimitive("user").getAsString();
                if(!lll.containsKey(username)){
                    LinkedList<String> l2 = new LinkedList<String>();
                    l2.add(coords.getAsJsonPrimitive("id").getAsString());
                    lll.put(username,l2);
                }else{
                    lll.get(username).add(coords.getAsJsonPrimitive("id").getAsString());
                }
            }
        }

        return lll;
    }

    private static void updateHBaseRuningJobTable(Map<String, List<String>> map) throws IOException {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",ZK);
        htable = new HTable(conf,"job_running");

        Iterator iter = map.entrySet().iterator();

        List<Put> lp = new LinkedList<Put>();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            System.out.println((String)entry.getKey());

            Put p = new Put(Bytes.toBytes((String)entry.getKey()));

            List<String> l1 = (List<String>)entry.getValue();
            for(String s:l1){
                p.add(Bytes.toBytes("r"),Bytes.toBytes(s),null);
            }
            lp.add(p);
        }
        htable.put(lp);
    }

    private static RunningStatus getRunningStatus(String jobID) throws IOException {
        String url = progress_url_prefix + jobID + progress_url_postfix;
        HttpGet getRequest = new HttpGet(url);
        getRequest.addHeader("accept", "application/json");
        HttpResponse response = httpClient.execute(getRequest);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        JsonObject joo = ee.getAsJsonObject();
        JsonArray jaa = joo.getAsJsonObject("jobs").getAsJsonArray("job");
        if(jaa.size() > 1){
            System.out.println("should no be here");
        }
        JsonElement type = jaa.get(0);
        JsonObject coords = type.getAsJsonObject();

        long mapsTotal = coords.getAsJsonPrimitive("mapsTotal").getAsLong();
        long mapsCompleted = coords.getAsJsonPrimitive("mapsCompleted").getAsLong();
        int mapProgress = coords.getAsJsonPrimitive("mapProgress").getAsInt();
        long reducesTotal = coords.getAsJsonPrimitive("reducesTotal").getAsLong();
        long reducesCompleted = coords.getAsJsonPrimitive("reducesCompleted").getAsLong();
        int  reduceProgress = coords.getAsJsonPrimitive("reduceProgress").getAsInt();
        long startTime = coords.getAsJsonPrimitive("startTime").getAsLong();
        long elapsedTime = coords.getAsJsonPrimitive("elapsedTime").getAsLong();

        double ttt = (((double) mapsCompleted+(double)reducesCompleted)/((double)mapsTotal+(double)reducesTotal));
        long ETA = startTime + (long)(((double)elapsedTime)/ttt);

        return new RunningStatus(mapProgress,reduceProgress,startTime,elapsedTime,ETA);

    }


    private static RunningStatus parse_json(){
        JsonElement ee = parser.parse(JSON_STR);
        JsonObject joo = ee.getAsJsonObject();
        JsonArray jaa = joo.getAsJsonObject("jobs").getAsJsonArray("job");

        if(jaa.size() > 1){
            System.out.println("should no be here");
        }
        JsonElement type = jaa.get(0);
        JsonObject coords = type.getAsJsonObject();

        long mapsTotal = coords.getAsJsonPrimitive("mapsTotal").getAsLong();
        long mapsCompleted = coords.getAsJsonPrimitive("mapsCompleted").getAsLong();
        int mapProgress = coords.getAsJsonPrimitive("mapProgress").getAsInt();
        long reducesTotal = coords.getAsJsonPrimitive("reducesTotal").getAsLong();
        long reducesCompleted = coords.getAsJsonPrimitive("reducesCompleted").getAsLong();
        int  reduceProgress = coords.getAsJsonPrimitive("reduceProgress").getAsInt();
        long startTime = coords.getAsJsonPrimitive("startTime").getAsLong();
        long elapsedTime = coords.getAsJsonPrimitive("elapsedTime").getAsLong();

        double ttt = (((double) mapsCompleted+(double)reducesCompleted)/((double)mapsTotal+(double)reducesTotal));
        long ETA = startTime + (long)(((double)elapsedTime)/ttt);

        return new RunningStatus(mapProgress,reduceProgress,startTime,elapsedTime,ETA);
    }

    private static void validateMap(Map<String, List<String>> map ){
        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            System.out.println((String)entry.getKey());
            List<String> l1 = (List<String>)entry.getValue();
            for(String s:l1){
                System.out.println(s);
            }
        }
    }
}