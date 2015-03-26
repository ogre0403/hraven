package org.nchc.rest;

import com.sun.jersey.api.json.JSONWithPadding;
import javax.ws.rs.*;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 1403035 on 2015/2/2.
 */

@Path("/api/jsonp/")
public class JSONPResource extends RestJSONResource{

    @GET
    @Path("job/{cluster}/{user}/{jobname}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobByTimeInterval(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @PathParam("jobname") String jobname,
            @DefaultValue("false")@QueryParam("counter") boolean counter,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time,
            @DefaultValue("10")@QueryParam("size") int size,
            @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
            @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobByTimeInterval(cluster,user,jobname,counter,start_time,end_time,size,sso_token),callback);
    }

    @GET
    @Path("job/{cluster}/{user}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobByTimeInterval(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @DefaultValue("false")@QueryParam("counter") boolean counter,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time,
            @DefaultValue("10")@QueryParam("size") int size,
            @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
            @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobByTimeInterval(cluster,user,counter,start_time,end_time,size,sso_token),callback);
    }


    @GET
    @Path("jobList/{cluster}/{user}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobList(@PathParam("cluster") String cluster,
                                   @PathParam("user") String user,
                                   @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
                                   @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobList(cluster,user,sso_token),callback);
    }


    @GET
    @Path("runList/{cluster}/{user}/{jobname}")
    @Produces({"application/javascript"})
    public JSONWithPadding getRunList(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @PathParam("jobname") String jobname,
            @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
            @QueryParam("callback") String callback)throws IOException {
        return new JSONWithPadding(getRunList(cluster,user,jobname,sso_token),callback);
    }


    @GET
    @Path("job/{cluster}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobById(@PathParam("cluster") String cluster,
                                 @QueryParam("jobId") String jobId,
                                 @DefaultValue("false")@QueryParam("counter") boolean counter,
                                 @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
                                 @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobById(cluster,jobId,counter,sso_token),callback);
    }


    @GET
    @Path("tasks/{cluster}/{jobId}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobTasksById(@PathParam("cluster") String cluster,
                                             @PathParam("jobId") String jobId,
                                             @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
                                             @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobTasksById(cluster,jobId,sso_token),callback);
    }


    @GET
    @Path("running/status/{jobId}")
    @Produces({"application/javascript"})
    public JSONWithPadding getRunStatus(@PathParam("jobId") String jobId,
                                        @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
                                        @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getRunStatus(jobId,sso_token),callback);
    }


    @GET
    @Path("running/id/{username}/{jobname}")
    @Produces({"application/javascript"})
    public JSONWithPadding GetRunningJobID(@PathParam("username")String username,
                                        @PathParam("jobname")String jobname,
                                        @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
                                        @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(GetRunningJobID(username,jobname,sso_token),callback);
    }



    @GET
    @Path("running/job/{username}")
    @Produces({"application/javascript"})
    public JSONWithPadding getRuningJobName(@PathParam("username") String username,
                                            @CookieParam("PUBLIC_APP_USER_SSO_TOKEN")String sso_token,
                                            @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getRuningJobName(username,sso_token),callback);
    }


}
