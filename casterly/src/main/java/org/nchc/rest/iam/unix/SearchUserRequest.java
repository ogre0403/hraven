package org.nchc.rest.iam.unix;

import org.nchc.rest.iam.IamConstants;

/**
 * Created by 1403035 on 2015/6/24.
 */
public class SearchUserRequest {
    private App_User_mapping_Relation APP_USER_MAPPING_RELATION;
    private String PRIVILEGED_APP_SSO_TOKEN;
    private String APP_UNIX_ACCOUNT_GROUP_UUID = IamConstants.APP_UNIX_ACCOUNT_GROUP_UUID;
    private String KEYWORD_COMPARISON = "EXACT";
    private String KEYWORD_RELATION = "ALL";

    public SearchUserRequest(String PRIVILEGED_APP_SSO_TOKEN,
                             String APP_USER_NODE_UUID,
                             String APP_COMPANY_UUID){
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
        APP_USER_MAPPING_RELATION = new App_User_mapping_Relation(APP_COMPANY_UUID,APP_USER_NODE_UUID);
    }

    public App_User_mapping_Relation getAPP_USER_MAPPING_RELATION() {
        return APP_USER_MAPPING_RELATION;
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

    class App_User_mapping_Relation{
        private String APP_USER_NODE_UUID;
        private String APP_COMPANY_UUID;
        public App_User_mapping_Relation(String company_uid, String user_node_uid){
            this.APP_COMPANY_UUID =  company_uid;
            this.APP_USER_NODE_UUID = user_node_uid;
        }

        public String getAPP_USER_NODE_UUID() {
            return APP_USER_NODE_UUID;
        }

        public String getAPP_COMPANY_UUID() {
            return APP_COMPANY_UUID;
        }
    }
}
