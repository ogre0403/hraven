package org.nchc.history;

import java.lang.reflect.Type;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.nchc.extend.ExtendConstants;
import org.nchc.rest.iam.IamConstants;
import org.nchc.rest.RunningStatusDAO;
import org.nchc.rest.iam.app.BasicRequest;
import org.nchc.rest.iam.app.UsernodeRequest;
import org.nchc.rest.iam.app.UuidRequest;
import org.nchc.rest.iam.unix.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.StringBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
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

    public static void main(String[] args) throws IOException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, URISyntaxException {
        TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] certificate, String authType) {
                return true;
            }
        };
        SSLSocketFactory sf = new SSLSocketFactory(acceptingTrustStrategy, new AllowAllHostnameVerifier());
        httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, sf));

        StringBuilder u = new StringBuilder();
        String[] u1 = new String[1];

        testIAM2("4e8bc932-99be-42ed-84ad-ce7d165b7912", u1);

        System.out.println("u1: " + u1[0]);


        /*
        String MAKER="\\$\\@MAKER\\@\\$";
        String ts = "aaaccc  <>   $@MAKER@$ asss <> sass   $@MAKER@$  sasdd";
        System.out.println(ts);

        String ss = ts.replaceAll(MAKER,"user");
        System.out.println(ss);

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
     //   parse_json();
    //    Map<String, List<String>> joblist =  getJobID();
    //    updateHBaseRuningJobTable(joblist);



    }

    private static void testIAM2(String SSO_TOKEN, String[] username)throws IOException, URISyntaxException{
        String PRIVILEGED_APP_SSO_TOKEN;
        String APP_USER_NODE_UUID;
        String APP_COMPANY_UUID;
        String APP_UNIX_USER_UUID;
        String APP_UNIX_GROUP_UUID;
        String UNIX_USERNAME;
        String UNIX_GID;

        String APP_UNIX_GROUP_NAME;
        List<String> APP_UNIX_GROUP_MEMBER_LIST;
        String error_msg ;

        Gson gson = new Gson();
        BasicRequest br = new BasicRequest();
        HttpPost httpPost = new HttpPost(IamConstants.IAM_BASCI_URL);
        httpPost.addHeader("accept", "application/json");
        StringEntity input = new StringEntity(gson.toJson(br));
        input.setContentType("application/json");
        httpPost.setEntity(input);
        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();


        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println(error_msg);
            return ;
        }else {
            PRIVILEGED_APP_SSO_TOKEN = ee.getAsJsonObject().get("PRIVILEGED_APP_SSO_TOKEN").getAsString();
        }

        UuidRequest uuid = new UuidRequest(PRIVILEGED_APP_SSO_TOKEN,SSO_TOKEN);
        httpPost.setURI(new URI(IamConstants.IAM_UUID_URL));
//        System.out.println(gson.toJson(uuid));
        StringEntity input2 = new StringEntity(gson.toJson(uuid));
        input2.setContentType("application/json");
        httpPost.setEntity(input2);
//        httpPost.setEntity(input2);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println(error_msg);
            return;
        }else {
            APP_USER_NODE_UUID = ee.getAsJsonObject().get("APP_USER_NODE_UUID").getAsString();
            APP_COMPANY_UUID = ee.getAsJsonObject().get("APP_COMPANY_UUID").getAsString();
            System.out.println("APP_USER_NODE_UUID: " + APP_USER_NODE_UUID);
            System.out.println("APP_COMPANY_UUID: "+APP_COMPANY_UUID);
        }




        SearchUserRequest suq = new SearchUserRequest(PRIVILEGED_APP_SSO_TOKEN,
                                                        APP_USER_NODE_UUID,
                                                        APP_COMPANY_UUID);
//        System.out.println(gson.toJson(suq));
        httpPost.setURI(new URI(IamConstants.IAM_SEARCH_UNIX_USER_URL));
        StringEntity input3 = new StringEntity(gson.toJson(suq));
        input3.setContentType("application/json");
        httpPost.setEntity(input3);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println("ERROR: "+error_msg);
            return;
        }else {
            APP_UNIX_USER_UUID = ee.getAsJsonObject().get("APP_UNIX_USER_UUID_LIST").getAsJsonArray().get(0).getAsString();
            System.out.println("APP_UNIX_GROUP_UUID: "+APP_UNIX_USER_UUID);
        }


        GetUserRequest gur = new GetUserRequest(PRIVILEGED_APP_SSO_TOKEN, APP_UNIX_USER_UUID);
//        System.out.println(gson.toJson(gur));
        httpPost.setURI(new URI(IamConstants.IAM_GET_UNIX_USER_URL));
        StringEntity input4 = new StringEntity(gson.toJson(gur));
        input4.setContentType("application/json");
        httpPost.setEntity(input4);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println("ERROR: "+error_msg);
            return;
        }else {
            UNIX_USERNAME = ee.getAsJsonObject().get("APP_UNIX_USER_BASIC_PROFILE")
                    .getAsJsonObject().get("UNIX_USERNAME").getAsString();
            username[0] = UNIX_USERNAME;
            UNIX_GID = ee.getAsJsonObject().get("APP_UNIX_USER_BASIC_PROFILE")
                    .getAsJsonObject().get("UNIX_GID").getAsString();
            System.out.println("UNIX_USERNAME: "+UNIX_USERNAME);
            System.out.println("UNIX_GID: "+UNIX_GID);
        }


        SearchGroupRequest sgr = new SearchGroupRequest(PRIVILEGED_APP_SSO_TOKEN,UNIX_GID);
//        System.out.println(gson.toJson(sgr));
        httpPost.setURI(new URI(IamConstants.IAM_SEARCH_UNIX_GRP_URL));
        StringEntity input5 = new StringEntity(gson.toJson(sgr));
        input5.setContentType("application/json");
        httpPost.setEntity(input5);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println("ERROR: "+error_msg);
            return;
        }else {
            APP_UNIX_GROUP_UUID = ee.getAsJsonObject().get("APP_UNIX_GROUP_UUID_LIST").getAsJsonArray().get(0).getAsString();
            System.out.println("APP_UNIX_GROUP_UUID: "+APP_UNIX_GROUP_UUID);
        }


        GetGroupRequest ggr = new GetGroupRequest(PRIVILEGED_APP_SSO_TOKEN,APP_UNIX_GROUP_UUID);
//        System.out.println(gson.toJson(ggr));
        httpPost.setURI(new URI(IamConstants.IAM_GET_UNIX_GRP_URL));
        StringEntity input6 = new StringEntity(gson.toJson(ggr));
        input6.setContentType("application/json");
        httpPost.setEntity(input6);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println("ERROR: "+error_msg);
            return;
        }else {
            APP_UNIX_GROUP_NAME = ee.getAsJsonObject().get("APP_UNIX_GROUP_BASIC_PROFILE").getAsJsonObject().get("APP_UNIX_GROUP_NAME").getAsString();
            System.out.println("APP_UNIX_GROUP_NAME: "+APP_UNIX_GROUP_NAME);

            Type listType = new TypeToken<List<String>>(){}.getType();
            APP_UNIX_GROUP_MEMBER_LIST = gson.fromJson(ee.getAsJsonObject().get("APP_UNIX_GROUP_MEMBER_LIST"),listType);
            for(String s:APP_UNIX_GROUP_MEMBER_LIST){
                System.out.println(s);
            }
        }


        GetUsersRequest gusr = new GetUsersRequest(PRIVILEGED_APP_SSO_TOKEN,APP_UNIX_GROUP_MEMBER_LIST);
//        System.out.println(gson.toJson(gusr));
        httpPost.setURI(new URI(IamConstants.IAM_GET_UNIX_USERS_URL));
        StringEntity input7 = new StringEntity(gson.toJson(gusr));
        input7.setContentType("application/json");
        httpPost.setEntity(input7);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println("ERROR: "+error_msg);
            return;
        }else {

            JsonArray types = ee.getAsJsonObject().get("APP_UNIX_USER_RESULT_LIST").getAsJsonArray();
            for(JsonElement je : types){
                String abc = je.getAsJsonObject().get("APP_UNIX_USER_BASIC_PROFILE").getAsJsonObject().get("UNIX_USERNAME").getAsString();
                System.out.println(abc);
            }

        }


    }

    private static String testIAM(String SSO_TOKEN) throws IOException, URISyntaxException {

        String PRIVILEGED_APP_SSO_TOKEN;
        String APP_USER_NODE_UUID;
        String APP_COMPANY_UUID;
        String APP_USER_LOGIN_ID;
        String error_msg ;

        Gson gson = new Gson();
        BasicRequest br = new BasicRequest();
        HttpPost httpPost = new HttpPost(IamConstants.IAM_BASCI_URL);
        httpPost.addHeader("accept", "application/json");
        StringEntity input = new StringEntity(gson.toJson(br));
        input.setContentType("application/json");
        httpPost.setEntity(input);
        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();


        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println(error_msg);
            return null;
        }else {
            PRIVILEGED_APP_SSO_TOKEN = ee.getAsJsonObject().get("PRIVILEGED_APP_SSO_TOKEN").getAsString();
        }

        UuidRequest uuid = new UuidRequest(PRIVILEGED_APP_SSO_TOKEN,SSO_TOKEN);
        httpPost.setURI(new URI(IamConstants.IAM_UUID_URL));
        System.out.println(gson.toJson(uuid));
        StringEntity input2 = new StringEntity(gson.toJson(uuid));
        input2.setContentType("application/json");
        httpPost.setEntity(input2);
        httpPost.setEntity(input2);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println(error_msg);
            return null;
        }else {
            APP_USER_NODE_UUID = ee.getAsJsonObject().get("APP_USER_NODE_UUID").getAsString();
            APP_COMPANY_UUID = ee.getAsJsonObject().get("APP_COMPANY_UUID").getAsString();
            System.out.println(APP_USER_NODE_UUID);
        }

        UsernodeRequest nn = new UsernodeRequest(PRIVILEGED_APP_SSO_TOKEN,SSO_TOKEN,
                APP_USER_NODE_UUID,APP_COMPANY_UUID);

        httpPost.setURI(new URI(IamConstants.IAM_USERNODE_URL));
        System.out.println(gson.toJson(nn));
        StringEntity input3 = new StringEntity(gson.toJson(nn));
        input3.setContentType("application/json");
        httpPost.setEntity(input3);
        httpPost.setEntity(input3);

        response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            error_msg = ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString();
            System.out.println(error_msg);
            return null;
        }else {
            APP_USER_LOGIN_ID = ee.getAsJsonObject().
                    get("APP_USER_BASIC_PROFILE").getAsJsonObject().
                    get("APP_USER_LOGIN_ID").getAsString();
            System.out.println(APP_USER_LOGIN_ID);
        }
        return null;
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

    private static RunningStatusDAO getRunningStatus(String jobID) throws IOException {
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

        return new RunningStatusDAO(mapProgress,reduceProgress,startTime,elapsedTime,ETA);

    }


    private static RunningStatusDAO parse_json(){
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

        return new RunningStatusDAO(mapProgress,reduceProgress,startTime,elapsedTime,ETA);
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
