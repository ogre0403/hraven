package org.nchc.rest;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
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
import org.nchc.rest.iam.BasicRequest;
import org.nchc.rest.iam.IamConstants;
import org.nchc.rest.iam.UsernodeRequest;
import org.nchc.rest.iam.UuidRequest;

import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

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

    private static DefaultHttpClient getSSLHttpClient() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
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
    public String index(@CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token) throws IOException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        // if no PUBLIC_APP_USER_SSO_TOKEN cookie, which means non-https and show normal index.html.
        // For security issue, should return error.html in production environment.
        if(sso_token == null)
            return IOUtils.toString(RestJSONResource.class.getClass().getResourceAsStream("/index.html"));

        String loginName = getLoginNameByCookie(sso_token);

        // show error page if IAM authentication fail
        // or show index page with authenticated user
        return loginName == null ?
                IOUtils.toString(RestJSONResource.class.getClass().getResourceAsStream("/error.html")):
                getIndex(loginName);
    }

    private String getIndex(String user) throws IOException {
        InputStream is = RestJSONResource.class.getClass().getResourceAsStream("/index_template");
        String template ="";
        try {
            template = IOUtils.toString(is);
        }catch (IOException ioe){
            LOG.error(ioe.toString());
        }finally {
            is.close();
        }
        return template.replaceAll(IamConstants.MARKER,user);
    }

    private String getLoginNameByCookie(String cookie) throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        LOG.info("cookie: " + cookie);

        DefaultHttpClient httpClient = getSSLHttpClient();
        BasicRequest br = new BasicRequest();
        UuidRequest uuid = handleBasicRequest(br,cookie,httpClient);
        UsernodeRequest user = handleUuidRequest(uuid,httpClient);
        String loginUser = handleUsernodeRequest(user, httpClient);
        httpClient.getConnectionManager().shutdown();
        return loginUser;
    }

    private UuidRequest handleBasicRequest(BasicRequest br, String cookie, DefaultHttpClient httpClient) throws IOException {
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

    private UsernodeRequest handleUuidRequest(UuidRequest uuid, DefaultHttpClient httpClient) throws IOException {
        if(uuid == null || httpClient == null)
            return null;
        String APP_COMPANY_UUID;
        String APP_USER_NODE_UUID;
        HttpPost httpPost = new HttpPost(IamConstants.IAM_UUID_URL);
        System.out.println(gson.toJson(uuid));
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
        System.out.println(gson.toJson(usernode));
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
}
