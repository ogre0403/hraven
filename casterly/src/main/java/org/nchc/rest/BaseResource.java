package org.nchc.rest;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.nchc.extend.ExtendConstants;
import org.nchc.rest.iam.app.BasicRequest;
import org.nchc.rest.iam.IamConstants;
import org.nchc.rest.iam.app.UsernodeRequest;
import org.nchc.rest.iam.app.UuidRequest;
import org.nchc.rest.iam.unix.*;

import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 1403035 on 2015/2/2.
 */

@Path("/")
public class BaseResource {
    private static final Log LOG = LogFactory.getLog(BaseResource.class);

    /**TODO:
     * Is gson thread-safe ?
     * */
    private static Gson gson = new Gson();
    private static JsonParser parser = new JsonParser();

    protected static final String SLASH = "/" ;
    protected static final Configuration HBASE_CONF = HBaseConfiguration.create();

    protected static final ThreadLocal<QueryJobService> queryThreadLocal =
            new ThreadLocal<QueryJobService>() {
                @Override
                protected QueryJobService initialValue() {
                    try {
                        LOG.info("Initializing queryJobService");
                        return new QueryJobService(HBASE_CONF);
                    } catch (IOException e) {
                        throw new RuntimeException("Could not initialize queryJobService", e);
                    }
                }
            };

    protected static QueryJobService getQueryService(){
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Returning QueryJobService %s bound to thread %s",
                    queryThreadLocal.get(), Thread.currentThread().getName()));
        }
        return queryThreadLocal.get();
    }

    protected static DefaultHttpClient getSSLHttpClient()
            throws UnrecoverableKeyException, NoSuchAlgorithmException,
            KeyStoreException, KeyManagementException {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] certificate, String authType) {
                return true;
            }
        };
        SSLSocketFactory sf = new SSLSocketFactory(acceptingTrustStrategy, new AllowAllHostnameVerifier());
        httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, sf));
        return httpClient;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/")
    public String index(@CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token)
            throws IOException, UnrecoverableKeyException, NoSuchAlgorithmException,
            KeyStoreException, KeyManagementException {

        // if no PUBLIC_APP_USER_SSO_TOKEN cookie, which means non-https and show normal index.html.
        // For security issue, should return error.html in production environment.

        if(sso_token == null){
            return ExtendConstants.isHttpEnable == true ?
                    IOUtils.toString(RestJSONResource.class.getClass().getResourceAsStream("/index.html")):
                    IOUtils.toString(RestJSONResource.class.getClass().getResourceAsStream("/error.html"));
        }


//        String loginName = getLoginNameByCookie(sso_token);

        String[] loginName = new String[1];
        List<String> members = getMemberList(sso_token,loginName);

        // if IAM authentication fail
        //      show error page
        // else
        //      if superuser login
        //          show index page
        //      else
        //          show index page with authenticated user
        return loginName[0] == null ?
                IOUtils.toString(RestJSONResource.class.getClass().getResourceAsStream("/error.html")):
                    loginName[0].equals(ExtendConstants.SUPERUSER) ?
                        IOUtils.toString(RestJSONResource.class.getClass().getResourceAsStream("/index.html")):
                        getIndex(loginName[0], members);
    }

    private String getIndex(String user, List<String> members) throws IOException {
        InputStream is = RestJSONResource.class.getClass().getResourceAsStream("/index_template");
        String template ="";
        try {
            template = IOUtils.toString(is);
        }catch (IOException ioe){
            LOG.error(ioe.toString());
        }finally {
            is.close();
        }

        if(members != null) {
            for(String s: members){
                LOG.info(s);
            }
        }
        return template.replaceAll(IamConstants.MARKER, user);
    }

    private String getLoginNameByCookie(String cookie)
            throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException,
            KeyManagementException, IOException {
        LOG.info("cookie: " + cookie);

        DefaultHttpClient httpClient = getSSLHttpClient();
        BasicRequest br = new BasicRequest();
        UuidRequest uuid = handleBasicRequest(br,cookie,httpClient);
        UsernodeRequest user = handleUuidRequest1(uuid, httpClient);
        String loginUser = handleUsernodeRequest(user, httpClient);
        httpClient.getConnectionManager().shutdown();
        return loginUser;
    }

    private List<String> getMemberList(String cookie, String[] user)
            throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException,
            KeyManagementException, IOException {
        LOG.info("cookie: " + cookie);

        String[] group = new String[1];
        List<String> members = null;

        DefaultHttpClient httpClient = getSSLHttpClient();
        BasicRequest br = new BasicRequest();
        UuidRequest uuid = handleBasicRequest(br,cookie,httpClient);
        SearchUserRequest suq = handleUuidRequest(uuid, httpClient);
        GetUserRequest guq = handleSearchUserRequest(suq, httpClient);
        SearchGroupRequest sgq = handleGetUserRequest(guq,user,httpClient);

        GetGroupRequest ggq = handleSearchGroupRequest(sgq, httpClient);
        GetUsersRequest gusq = handleGetGroupRequest(ggq, group, httpClient);
        if(user[0] != null && group[0]!=null &&
                user[0].equals(group[0])){
            members = handleGetUsersRequest(gusq,httpClient);
        }
        httpClient.getConnectionManager().shutdown();

        return members;
    }


    protected UuidRequest handleBasicRequest(BasicRequest br, String cookie, DefaultHttpClient httpClient)
            throws IOException {
        if(br == null || cookie == null || httpClient == null){
            return null;
        }

        String PRIVILEGED_APP_SSO_TOKEN;
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
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            PRIVILEGED_APP_SSO_TOKEN = ee.getAsJsonObject().get("PRIVILEGED_APP_SSO_TOKEN").getAsString();
        }

        return  new UuidRequest(PRIVILEGED_APP_SSO_TOKEN,cookie);
    }

    private UsernodeRequest handleUuidRequest1(UuidRequest uuid, DefaultHttpClient httpClient) throws IOException {
        if(uuid == null || httpClient == null)
            return null;
        String APP_COMPANY_UUID;
        String APP_USER_NODE_UUID;
        HttpPost httpPost = new HttpPost(IamConstants.IAM_UUID_URL);
        StringEntity input2 = new StringEntity(gson.toJson(uuid));
        input2.setContentType("application/json");
        httpPost.setEntity(input2);
        httpPost.setEntity(input2);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            APP_USER_NODE_UUID = ee.getAsJsonObject().get("APP_USER_NODE_UUID").getAsString();
            APP_COMPANY_UUID = ee.getAsJsonObject().get("APP_COMPANY_UUID").getAsString();
        }

        UsernodeRequest usernode = new UsernodeRequest(
                uuid.getPRIVILEGED_APP_SSO_TOKEN(),
                uuid.getPUBLIC_APP_USER_SSO_TOKEN_TO_QUERY(),
                APP_USER_NODE_UUID,
                APP_COMPANY_UUID);
        return usernode;
    }

    private String handleUsernodeRequest(UsernodeRequest usernode, DefaultHttpClient httpClient) throws IOException {
        if (usernode == null || httpClient == null)
            return null;

        String APP_USER_LOGIN_ID;
        HttpPost httpPost = new HttpPost(IamConstants.IAM_USERNODE_URL);
        StringEntity input3 = new StringEntity(gson.toJson(usernode));
        input3.setContentType("application/json");
        httpPost.setEntity(input3);
        httpPost.setEntity(input3);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            APP_USER_LOGIN_ID = ee.getAsJsonObject().
                    get("APP_USER_BASIC_PROFILE").getAsJsonObject().
                    get("APP_USER_LOGIN_ID").getAsString();
        }
        return  APP_USER_LOGIN_ID;
    }

    private SearchUserRequest handleUuidRequest(UuidRequest uuid, DefaultHttpClient httpClient) throws IOException{
        if(uuid == null || httpClient == null)
            return null;
        String APP_COMPANY_UUID;
        String APP_USER_NODE_UUID;
        HttpPost httpPost = new HttpPost(IamConstants.IAM_UUID_URL);
        StringEntity input2 = new StringEntity(gson.toJson(uuid));
        input2.setContentType("application/json");
        httpPost.setEntity(input2);
        httpPost.setEntity(input2);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            APP_USER_NODE_UUID = ee.getAsJsonObject().get("APP_USER_NODE_UUID").getAsString();
            APP_COMPANY_UUID = ee.getAsJsonObject().get("APP_COMPANY_UUID").getAsString();
        }

        return new SearchUserRequest(
                uuid.getPRIVILEGED_APP_SSO_TOKEN(),
                APP_USER_NODE_UUID,
                APP_COMPANY_UUID);
    }

    private GetUserRequest handleSearchUserRequest(SearchUserRequest suq, DefaultHttpClient httpClient) throws IOException{
        if(suq == null || httpClient == null)
            return null;
        String APP_UNIX_USER_UUID;

        HttpPost httpPost = new HttpPost(IamConstants.IAM_SEARCH_UNIX_USER_URL);
        StringEntity input2 = new StringEntity(gson.toJson(suq));
        input2.setContentType("application/json");
        httpPost.setEntity(input2);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            JsonArray ja = ee.getAsJsonObject().get("APP_UNIX_USER_UUID_LIST").getAsJsonArray();
            if(ja.size() == 0){
                LOG.error("User uuid not found");
                return null;
            }else if(ja.size() > 1){
                LOG.error("Should be only ONE user uuid");
                return null;
            }else{
                APP_UNIX_USER_UUID = ja.get(0).getAsString();
            }
        }

        return new GetUserRequest(suq.getPRIVILEGED_APP_SSO_TOKEN(), APP_UNIX_USER_UUID);
    }

    private SearchGroupRequest handleGetUserRequest(GetUserRequest guq, String[] user,
                                                    DefaultHttpClient httpClient) throws IOException{
        if(guq == null || httpClient == null)
            return null;
        String UNIX_GID;

        HttpPost httpPost = new HttpPost(IamConstants.IAM_GET_UNIX_USER_URL);
        StringEntity input4 = new StringEntity(gson.toJson(guq));
        input4.setContentType("application/json");
        httpPost.setEntity(input4);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            user[0] = ee.getAsJsonObject().get("APP_UNIX_USER_BASIC_PROFILE")
                    .getAsJsonObject().get("UNIX_USERNAME").getAsString();

            UNIX_GID = ee.getAsJsonObject().get("APP_UNIX_USER_BASIC_PROFILE")
                    .getAsJsonObject().get("UNIX_GID").getAsString();
        }

        return  new SearchGroupRequest(guq.getPRIVILEGED_APP_SSO_TOKEN(),UNIX_GID);
    }

    private GetGroupRequest handleSearchGroupRequest(SearchGroupRequest srq,
                                                     DefaultHttpClient httpClient)throws IOException{
        if(srq == null || httpClient == null)
            return null;
        String APP_UNIX_GROUP_UUID;

        HttpPost httpPost = new HttpPost(IamConstants.IAM_SEARCH_UNIX_GRP_URL);
        StringEntity input5 = new StringEntity(gson.toJson(srq));
        input5.setContentType("application/json");
        httpPost.setEntity(input5);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            JsonArray ja = ee.getAsJsonObject().get("APP_UNIX_GROUP_UUID_LIST").getAsJsonArray();
            if(ja.size() == 0){
                LOG.error("Group uuid not found");
                return null;
            }else if(ja.size() > 1){
                LOG.error("Should be only ONE group uuid");
                return null;
            }else{
                APP_UNIX_GROUP_UUID = ja.get(0).getAsString();
            }
        }

        return new GetGroupRequest(srq.getPRIVILEGED_APP_SSO_TOKEN(),APP_UNIX_GROUP_UUID);
    }


    private GetUsersRequest handleGetGroupRequest(GetGroupRequest ggq,
                                                  String[] group,
                                                  DefaultHttpClient httpClient)throws IOException{
        if(ggq == null || httpClient == null)
            return null;
        List<String> APP_UNIX_GROUP_MEMBER_LIST;
        HttpPost httpPost = new HttpPost(IamConstants.IAM_GET_UNIX_GRP_URL);
        StringEntity input6 = new StringEntity(gson.toJson(ggq));
        input6.setContentType("application/json");
        httpPost.setEntity(input6);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        }else {
            group[0] = ee.getAsJsonObject().get("APP_UNIX_GROUP_BASIC_PROFILE").getAsJsonObject().get("APP_UNIX_GROUP_NAME").getAsString();

            Type listType = new TypeToken<List<String>>(){}.getType();
            APP_UNIX_GROUP_MEMBER_LIST = gson.fromJson(ee.getAsJsonObject().get("APP_UNIX_GROUP_MEMBER_LIST"),listType);
        }

        return new GetUsersRequest(ggq.getPRIVILEGED_APP_SSO_TOKEN(), APP_UNIX_GROUP_MEMBER_LIST);
    }

    private List<String> handleGetUsersRequest(GetUsersRequest guq,
                                           DefaultHttpClient httpClient)throws IOException{
        if(guq == null || httpClient == null)
            return null;
        LinkedList<String> userList = new LinkedList<String>();

        HttpPost httpPost = new HttpPost(IamConstants.IAM_GET_UNIX_USERS_URL);
        StringEntity input7 = new StringEntity(gson.toJson(guq));
        input7.setContentType("application/json");
        httpPost.setEntity(input7);

        HttpResponse response = httpClient.execute(httpPost);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatusLine().getStatusCode());
        }

        JsonElement ee = parser.parse(new InputStreamReader((response.getEntity().getContent())));
        String code = ee.getAsJsonObject().get("ERROR_CODE").getAsString();

        if (!code.equals("0")){
            LOG.error(ee.getAsJsonObject().get("ERROR_MESSAGE").getAsString());
            return null;
        } else {
            JsonArray types = ee.getAsJsonObject().get("APP_UNIX_USER_RESULT_LIST").getAsJsonArray();
            for(JsonElement je : types){
                userList.add(je.getAsJsonObject().get("APP_UNIX_USER_BASIC_PROFILE").getAsJsonObject().get("UNIX_USERNAME").getAsString());
            }
        }

        return userList;
    }

}
