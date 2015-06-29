package org.nchc.rest.iam.unix;

import org.nchc.rest.iam.IamConstants;

/**
 * Created by 1403035 on 2015/6/24.
 */
public class GetUserRequest {
    private String PRIVILEGED_APP_SSO_TOKEN;
    private String APP_UNIX_ACCOUNT_GROUP_UUID = IamConstants.APP_UNIX_ACCOUNT_GROUP_UUID;
    private String APP_UNIX_USER_UUID;

    public String getPRIVILEGED_APP_SSO_TOKEN() {
        return PRIVILEGED_APP_SSO_TOKEN;
    }

    public String getAPP_UNIX_ACCOUNT_GROUP_UUID() {
        return APP_UNIX_ACCOUNT_GROUP_UUID;
    }

    public String getAPP_UNIX_USER_UUID() {
        return APP_UNIX_USER_UUID;
    }

    public GetUserRequest(String PRIVILEGED_APP_SSO_TOKEN,
                          String APP_UNIX_USER_UUID){
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
        this.APP_UNIX_USER_UUID = APP_UNIX_USER_UUID;
    }
}
