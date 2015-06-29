package org.nchc.rest.iam.unix;

import org.nchc.rest.iam.IamConstants;

/**
 * Created by 1403035 on 2015/6/24.
 */
public class SearchGroupRequest {
    private String PRIVILEGED_APP_SSO_TOKEN;
    private String APP_UNIX_ACCOUNT_GROUP_UUID = IamConstants.APP_UNIX_ACCOUNT_GROUP_UUID;
    private String KEYWORD_COMPARISON = "PARTIAL";
    private String KEYWORD_RELATION="ALL";
    private App_Unix_Group_Basic_Profile APP_UNIX_GROUP_BASIC_PROFILE;

    public SearchGroupRequest(String PRIVILEGED_APP_SSO_TOKEN,
                              String APP_UNIX_GROUP_GID){
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
        this.APP_UNIX_GROUP_BASIC_PROFILE = new App_Unix_Group_Basic_Profile(APP_UNIX_GROUP_GID);

    }

    public String getPRIVILEGED_APP_SSO_TOKEN() {
        return PRIVILEGED_APP_SSO_TOKEN;
    }

    public String getAPP_UNIX_ACCOUNT_GROUP_UUID() {
        return APP_UNIX_ACCOUNT_GROUP_UUID;
    }

    public String getKEYWORD_COMPARISON() {
        return KEYWORD_COMPARISON;
    }

    public String getKEYWORD_RELATION() {
        return KEYWORD_RELATION;
    }

    public App_Unix_Group_Basic_Profile getAPP_UNIX_GROUP_BASIC_PROFILE() {
        return APP_UNIX_GROUP_BASIC_PROFILE;
    }

    class App_Unix_Group_Basic_Profile{
        private String APP_UNIX_GROUP_GID;
        public  App_Unix_Group_Basic_Profile(String APP_UNIX_GROUP_GID){
            this.APP_UNIX_GROUP_GID = APP_UNIX_GROUP_GID;
        }

        public String getAPP_UNIX_GROUP_GID() {
            return APP_UNIX_GROUP_GID;
        }
    }
}
