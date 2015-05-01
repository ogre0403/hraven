package org.nchc.rest;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by 1403035 on 2014/12/31.
 */
public class UserJobDAO {

    String username;
    String jobname;

    @JsonCreator
    public UserJobDAO(@JsonProperty("username") String u,
                      @JsonProperty("jobname") String j){
        this.username = u;
        this.jobname = j;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getJobname() {
        return jobname;
    }

    public void setJobname(String jobname) {
        this.jobname = jobname;
    }
}
