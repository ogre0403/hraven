package org.nchc.rest.iam;

/**
 * Created by 1403035 on 2015/2/26.
 */
public class IamConstants {

    private static final String IAM_HOST = "https://iam-api.nchc.org.tw/";

    /**
     *  REQUEST -------------------------------
     *  {
     *      "APP_PRIVATE_ID": "c183df7a1f37423b9e4bcd091153b808",
     *      "APP_PRIVATE_PASSWD": "c629f93af7524a95a1a46b65f39b42f7"
     *  }
     *
     *  RESPONSE -------------------------------
     *  {
     *      "ERROR_CODE": "0",
     *      "PUBLIC_APP_SSO_TOKEN": "ec5d2bfc-1e8d-4766-b37f-e0d09c1a3657",
     *      "PRIVILEGED_APP_SSO_TOKEN": "ad468055-9f07-47e4-9170-5821f642dac8",
     *      "PRIVATE_APP_SSO_TOKEN": "636a6412-7e07-442e-a4fc-a6ee76b6a3c3",
     *      "PUBLIC_APP_SSO_TOKEN_EXPIRY_DATE": "20150206110124+0800",
     *      "PRIVILEGED_APP_SSO_TOKEN_EXPIRY_DATE": "20150207090124+0800",
     *      "PRIVATE_APP_SSO_TOKEN_EXPIRY_DATE": "20150209090124+0800"
     *  }
     **/
    private static final String IAM_BASIC_ENTRY = "app/request_basic_authentication/";
    public static final String IAM_BASCI_URL = IAM_HOST+IAM_BASIC_ENTRY;

    /**
     *REQUEST -------------------------------
     *  {
     *      "PRIVILEGED_APP_SSO_TOKEN":"UUID GET FROM IAM_BASIC_ENTRY",
     *      "PUBLIC_APP_USER_SSO_TOKEN_TO_QUERY":"UUID GET FROM COOKIE"
     *  }
     *
     *RESPONSE -------------------------------
     *  {
     *      "ERROR_CODE": "0",
     *      "APP_USER_NODE_UUID": "2f746f3f-416b-4685-b702-2c0561f836e5",
     *      "APP_COMPANY_UUID": "0ba546de-e925-4f77-bb70-c9b042e15df6"
     *  }
     */
    private static final String IAM_UUID_ENTRY = "app_user/get_node_uuid/";
    public static final String IAM_UUID_URL = IAM_HOST+IAM_UUID_ENTRY;

    /**
     *REQUEST -------------------------------
     *  {
     *      "PRIVILEGED_APP_SSO_TOKEN":"UUID GOT FROM IAM_BASIC_ENTRY",
     *      "PUBLIC_APP_USER_SSO_TOKEN":"UUID FROM COOKIE",
     *      "APP_USER_NODE_UUID":"UUID GOT FROM IAM_UUID_ENTRY",
     *      "APP_COMPANY_UUID":"UUID GOT FROM IAM_UUID_ENTRY",
     *      "APP_USER_BASIC_PROFILE":{
     *          "APP_USER_LOGIN_ID":""
     *      }
     *  }
     *
     *RESPONSE -------------------------------
     *  {
     *      "ERROR_CODE": 0,
     *      "APP_USER_NODE_UUID": "2f746f3f-416b-4685-b702-2c0561f836e5",
     *      "APP_USER_BASIC_PROFILE": {
     *          "APP_USER_LOGIN_ID": "admin",
     *      },
     *      "APP_USER_NODE_LAST_UPDATE_TIME": "20141231054126+0800",
     *      "APP_USER_NODE_LAST_UPDATE_TAG": "3c92a45169321588838f9697032d8dbc"
     *  }
     */
    private static final String IAM_USERNODE_ENTRY = "org_tree_surrogate/get_user_node/";
    public static final String IAM_USERNODE_URL = IAM_HOST+IAM_USERNODE_ENTRY;

    public static final String MARKER="\\$\\@MARKER\\@\\$";

}
