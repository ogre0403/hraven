package org.nchc.rest.iam.unix;

import org.nchc.rest.iam.IamConstants;

import java.util.List;

/**
 * Created by 1403035 on 2015/6/24.
 */
public class GetUsersRequest {
    private String PRIVILEGED_APP_SSO_TOKEN;
    private String APP_UNIX_ACCOUNT_GROUP_UUID = IamConstants.APP_UNIX_ACCOUNT_GROUP_UUID;
    private List<String> APP_UNIX_USER_UUID_LIST;

    public GetUsersRequest(String PRIVILEGED_APP_SSO_TOKEN,
                           List<String> APP_UNIX_USER_UUID_LIST){
        this.PRIVILEGED_APP_SSO_TOKEN = PRIVILEGED_APP_SSO_TOKEN;
        this.APP_UNIX_USER_UUID_LIST = APP_UNIX_USER_UUID_LIST;
    }

}
