package org.nchc.rest.iam.app;

/**
 * Created by 1403035 on 2015/3/2.
 */
public class UsernodeRequest {

    private String PRIVILEGED_APP_SSO_TOKEN;
    private String PUBLIC_APP_USER_SSO_TOKEN;
    private String APP_USER_NODE_UUID;
    private String APP_COMPANY_UUID;

    private App_User_Basic_Profile APP_USER_BASIC_PROFILE = new App_User_Basic_Profile();

    public UsernodeRequest(String PRIVILEGED_APP_SSO_TOKEN,
                           String PUBLIC_APP_USER_SSO_TOKEN,
                           String APP_USER_NODE_UUID,
                           String APP_COMPANY_UUID
                           ){
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
        this.PUBLIC_APP_USER_SSO_TOKEN = PUBLIC_APP_USER_SSO_TOKEN;
        this.APP_USER_NODE_UUID = APP_USER_NODE_UUID;
        this.APP_COMPANY_UUID = APP_COMPANY_UUID;
    }

    public String getPRIVILEGED_APP_SSO_TOKEN() {
        return PRIVILEGED_APP_SSO_TOKEN;
    }

    public void setPRIVILEGED_APP_SSO_TOKEN(String PRIVILEGED_APP_SSO_TOKEN) {
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
    }

    public String getPUBLIC_APP_USER_SSO_TOKEN() {
        return PUBLIC_APP_USER_SSO_TOKEN;
    }

    public void setPUBLIC_APP_USER_SSO_TOKEN(String PUBLIC_APP_USER_SSO_TOKEN) {
        this.PUBLIC_APP_USER_SSO_TOKEN = PUBLIC_APP_USER_SSO_TOKEN;
    }

    public String getAPP_USER_NODE_UUID() {
        return APP_USER_NODE_UUID;
    }

    public void setAPP_USER_NODE_UUID(String APP_USER_NODE_UUID) {
        this.APP_USER_NODE_UUID = APP_USER_NODE_UUID;
    }

    public String getAPP_COMPANY_UUID() {
        return APP_COMPANY_UUID;
    }

    public void setAPP_COMPANY_UUID(String APP_COMPANY_UUID) {
        this.APP_COMPANY_UUID = APP_COMPANY_UUID;
    }

    class App_User_Basic_Profile {
        private String APP_USER_LOGIN_ID;

        public App_User_Basic_Profile(){
            this.APP_USER_LOGIN_ID="";
        }

        public String getAPP_USER_LOGIN_ID() {
            return APP_USER_LOGIN_ID;
        }

        public void setAPP_USER_LOGIN_ID(String APP_USER_LOGIN_ID) {
            this.APP_USER_LOGIN_ID = APP_USER_LOGIN_ID;
        }
    }
}
