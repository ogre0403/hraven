package org.nchc.rest.iam;

/**
 * Created by 1403035 on 2015/3/2.
 */
public class UuidRequest {
    private String PRIVILEGED_APP_SSO_TOKEN;
    private String PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY;

    public UuidRequest(String PRIVILEGED_APP_SSO_TOKEN,  String PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY){
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
        this.PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY = PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY;
    }

    public String getPRIVILEGED_APP_SSO_TOKEN() {
        return PRIVILEGED_APP_SSO_TOKEN;
    }

    public void setPRIVILEGED_APP_SSO_TOKEN(String PRIVILEGED_APP_SSO_TOKEN) {
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
    }

    public String getPUBLIC_APP_USER_SSO_TOKEN_TO_QUERY() {
        return PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY;
    }

    public void setPUBLIC_APP_USER_SSO_TOKEN_TO_QUERY(String PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY) {
        this.PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY = PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY;
    }
}
