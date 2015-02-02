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
            @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobByTimeInterval(cluster,user,jobname,counter,start_time,end_time),callback);
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
            @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobByTimeInterval(cluster,user,counter,start_time,end_time),callback);
    }


    @GET
    @Path("jobList/{cluster}/{user}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobList(@PathParam("cluster") String cluster,
                                   @PathParam("user") String user,
                                   @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobList(cluster,user),callback);
    }


    @GET
    @Path("runList/{cluster}/{user}/{jobname}")
    @Produces({"application/javascript"})
    public JSONWithPadding getRunList(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @PathParam("jobname") String jobname,
            @QueryParam("callback") String callback)throws IOException {
        return new JSONWithPadding(getRunList(cluster,user,jobname),callback);
    }


    @GET
    @Path("job/{cluster}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobById(@PathParam("cluster") String cluster,
                                 @QueryParam("jobId") String jobId,
                                 @DefaultValue("false")@QueryParam("counter") boolean counter,
                                 @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobById(cluster,jobId,counter),callback);
    }


    @GET
    @Path("tasks/{cluster}/{jobId}")
    @Produces({"application/javascript"})
    public JSONWithPadding getJobTasksById(@PathParam("cluster") String cluster,
                                             @PathParam("jobId") String jobId,
                                             @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getJobTasksById(cluster,jobId),callback);
    }


    @GET
    @Path("running/status/{jobId}")
    @Produces({"application/javascript"})
    public JSONWithPadding getRunStatus(@PathParam("jobId") String jobId,
                                         @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getRunStatus(jobId),callback);
    }


    @GET
    @Path("running/id/{username}/{jobname}")
    @Produces({"application/javascript"})
    public JSONWithPadding GetRunningJobID(@PathParam("username")String username,
                                        @PathParam("jobname")String jobname,
                                        @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(GetRunningJobID(username,jobname),callback);
    }



    @GET
    @Path("running/job/{username}")
    @Produces({"application/javascript"})
    public JSONWithPadding getRuningJobName(@PathParam("username") String username,
                                            @QueryParam("callback") String callback) throws IOException {
        return new JSONWithPadding(getRuningJobName(username),callback);
    }


    @GET
    @Path("jsonp1")
    @Produces({"application/javascript"})
    public JSONWithPadding testJSONP1(@QueryParam("callback") String callback){
        List<String> ls = new LinkedList<String>();
        ls.add("aaa");
        ls.add("bbb");
        return new JSONWithPadding(ls, callback);
    }

    @GET
    @Path("jsonp2")
    @Produces({"application/javascript"})
    public JSONWithPadding testJSONP2(@QueryParam("callback") String callback){
        RunningStatusDAO dao = new RunningStatusDAO();
        return new JSONWithPadding(dao, callback);
    }

    @GET
    @Path("jsonp3")
    @Produces({"application/javascript"})
    public JSONWithPadding testJSONP3(@QueryParam("callback") String callback){
        RunningStatusDAO dao1 = new RunningStatusDAO(1,1,1,1,1);

        RunningStatusDAO dao2 = new RunningStatusDAO(2,2,2,2,2);
        RunningStatusDAO dao3 = new RunningStatusDAO(3,3,3,3,3);
        LinkedList<RunningStatusDAO> ll = new LinkedList<RunningStatusDAO>();
        ll.add(dao1);
        ll.add(dao2);
        ll.add(dao3);
        return new JSONWithPadding(ll, callback);
    }

}
