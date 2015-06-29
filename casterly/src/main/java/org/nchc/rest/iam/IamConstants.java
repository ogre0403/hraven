package org.nchc.rest.iam;

/**
 * Created by 1403035 on 2015/2/26.
 */
public class IamConstants {

    public static final String APP_UNIX_ACCOUNT_GROUP_UUID = "40a7b7b2-5b7f-4d69-b59e-88e79d40d50a";
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


    /**
     * REQUEST -------------------------------
     *{
        "PRIVILEGED_APP_SSO_TOKEN": "15413892-a9d4-4473-9d37-9a12e843608a",
        "APP_UNIX_ACCOUNT_GROUP_UUID":"40a7b7b2-5b7f-4d69-b59e-88e79d40d50a",
        "KEYWORD_COMPARISON":"EXACT",
        "KEYWORD_RELATION":"ALL",
        "APP_USER_MAPPING_RELATION":{
            "APP_USER_NODE_UUID": "02ab9cac-f381-4f50-840e-7953070f38f9",
            "APP_COMPANY_UUID": "0ba546de-e925-4f77-bb70-c9b042e15df6"
        }
     }

     *RESPONSE -------------------------------
     {
        "ERROR_CODE": 0,
        "APP_UNIX_USER_UUID_LIST": [
        "29b66172-7154-450c-a599-efa3d3e9b2c0"
        ]
     }
     **/
    private static final String IAM_SEARCH_UNIX_USER = "unix_user/search_unix_user/";
    public static final String IAM_SEARCH_UNIX_USER_URL = IAM_HOST + IAM_SEARCH_UNIX_USER;

    /**
     *
     {
        "PRIVILEGED_APP_SSO_TOKEN": "15413892-a9d4-4473-9d37-9a12e843608a",
        "APP_UNIX_ACCOUNT_GROUP_UUID":"40a7b7b2-5b7f-4d69-b59e-88e79d40d50a",
        "APP_UNIX_USER_UUID":"29b66172-7154-450c-a599-efa3d3e9b2c0"
     }

     {
        "ERROR_CODE": "0",
        "APP_UNIX_USER_BASIC_PROFILE": {
            "UNIX_USERNAME": "v00scl00",
            "UNIX_ACCOUNT_STATUS": "1",
            "UNIX_UID": "13750",
            "UNIX_INFO": "陸聲忠/2015060001/crt-date 20150623 15:05:35",
            "UNIX_GID": "3315",
            "UNIX_HOME_DIRECTORY": "/home/v00scl00",
            "UNIX_SHELL": "/bin/bash"
        },
        "APP_UNIX_GROUP_BELONG_TO": [
            "1c3f13b9-5da7-4e46-b395-0bcd2fd976a3"
        ],
        "APP_USER_MAPPING_RELATION": {
            "APP_COMPANY_UUID": "0ba546de-e925-4f77-bb70-c9b042e15df6",
            "APP_USER_NODE_UUID": "02ab9cac-f381-4f50-840e-7953070f38f9"
        },
        "APP_UNIX_USER_LAST_UPDATE_TIME": "20150623150541+0800",
        "APP_UNIX_USER_LAST_UPDATE_TAG": "ccf01a26a4651681c94e11598b2cde6b"
     }
     **/
    private static final String IAM_GET_UNIX_USER = "unix_user/get_unix_user/";
    public static final String IAM_GET_UNIX_USER_URL = IAM_HOST+IAM_GET_UNIX_USER;

    /**
     *
     {
        "PRIVILEGED_APP_SSO_TOKEN": "15413892-a9d4-4473-9d37-9a12e843608a",
        "APP_UNIX_ACCOUNT_GROUP_UUID":"40a7b7b2-5b7f-4d69-b59e-88e79d40d50a",
        "APP_UNIX_USER_UUID_LIST":[
            "246db40a-4223-4962-b750-b2a354adad4e",
            "32c87d08-7533-4586-936c-34af310a3352",
            "29b66172-7154-450c-a599-efa3d3e9b2c0",
            "9974cc49-184b-47fd-b65d-6acbc5eb5014"
     ]

     }
     **/
    private static final String IAM_GET_UNIX_USERS = "unix_user/get_unix_users/";
    public static final String IAM_GET_UNIX_USERS_URL = IAM_HOST+IAM_GET_UNIX_USERS;

    /**
     *
     {
        "PRIVILEGED_APP_SSO_TOKEN": "15413892-a9d4-4473-9d37-9a12e843608a",
        "APP_UNIX_ACCOUNT_GROUP_UUID":"40a7b7b2-5b7f-4d69-b59e-88e79d40d50a",
        "KEYWORD_COMPARISON":"PARTIAL",
        "KEYWORD_RELATION":"ALL",
        "APP_UNIX_GROUP_BASIC_PROFILE":
            {
                "APP_UNIX_GROUP_GID":"3315"
            }
     }

     {
        "ERROR_CODE": 0,
        "APP_UNIX_GROUP_UUID_LIST": [
            "1c3f13b9-5da7-4e46-b395-0bcd2fd976a3"
        ]
     }
     * */
    private static final String IAM_SEARCH_UNIX_GRP = "unix_group/search_unix_group/";
    public static final String IAM_SEARCH_UNIX_GRP_URL = IAM_HOST + IAM_SEARCH_UNIX_GRP;


    /**
     *
     {
        "PRIVILEGED_APP_SSO_TOKEN": "15413892-a9d4-4473-9d37-9a12e843608a",
        "APP_UNIX_ACCOUNT_GROUP_UUID": "40a7b7b2-5b7f-4d69-b59e-88e79d40d50a",
        "APP_UNIX_GROUP_UUID": "1c3f13b9-5da7-4e46-b395-0bcd2fd976a3"
     }
     {
        "ERROR_CODE": "0",
        "APP_UNIX_GROUP_BASIC_PROFILE": {
            "APP_UNIX_GROUP_GID": "3315",
            "APP_UNIX_GROUP_NAME": "v00zyw01",
            "APP_UNIX_GROUP_DESC": "王柔懿/2015060001/crtda 20150618 16:34:"
        },
        "APP_UNIX_GROUP_MEMBER_LIST": [
            "246db40a-4223-4962-b750-b2a354adad4e",
            "32c87d08-7533-4586-936c-34af310a3352",
            "29b66172-7154-450c-a599-efa3d3e9b2c0",
            "9974cc49-184b-47fd-b65d-6acbc5eb5014"
        ],
        "APP_UNIX_GROUP_LAST_UPDATE_TIME": "20150618163459+0800",
        "APP_UNIX_GROUP_LAST_UPDATE_TAG": "ff75d4475445926466355aca8be0c1a3"
     }
     **/
    private static final String IAM_GET_UNIX_GRP = "unix_group/get_unix_group/";
    public static final String IAM_GET_UNIX_GRP_URL = IAM_HOST + IAM_GET_UNIX_GRP;

    public static final String MARKER="\\$\\@MARKER\\@\\$";
    public static final String start_tag ="<option value=\"";
    public static final String mid_tag = "\">";
    public static final String end_tag = "</option>";

}
