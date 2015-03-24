/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package org.nchc.rest;

import com.google.common.base.Stopwatch;
import com.twitter.hraven.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main REST resource that handles binding the REST API to the JobHistoryService.
 */
@Path("/api/v1/")
public class RestJSONResource extends BaseResource{
  private static final Log LOG = LogFactory.getLog(RestJSONResource.class);



    @GET
    @Path("job/{cluster}/{user}/{jobname}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JobDetails> getJobByTimeInterval(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @PathParam("jobname") String jobname,
            @DefaultValue("false")@QueryParam("counter") boolean counter,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time,
            @DefaultValue("10")@QueryParam("size") int size) throws IOException {
        if(start_time < 0 || end_time < 0 || start_time > end_time ) {
            // given incorrect Time interval or time interval not set, return all Job runs
            return getQueryService().getCertainJobAllRuns(cluster,user,jobname,counter,size);
        }
        return getQueryService().getCertainJobRunsInTimeInterval(cluster,user,jobname,start_time,end_time,counter,size);
    }


    @GET
    @Path("job/{cluster}/{user}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JobDetails> getJobByTimeInterval(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @DefaultValue("false")@QueryParam("counter") boolean counter,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time,
            @DefaultValue("10")@QueryParam("size") int size) throws IOException {
        if(start_time < 0 || end_time < 0 || start_time > end_time ) {
            // given incorrect Time interval or time interval not set, return all Job runs
            return getQueryService().getAllJobInTimeInterval(cluster,user,counter,size);
        }
        return getQueryService().getAllJobInTimeInterval(cluster,user,start_time,end_time,counter,size);
    }

    @GET
    @Path("jobList/{cluster}/{user}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getJobList(@PathParam("cluster") String cluster,
                                   @PathParam("user") String user) throws IOException {
        return getQueryService().getAllJobName(cluster,user);
    }

    @GET
    @Path("runList/{cluster}/{user}/{jobname}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getRunList(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @PathParam("jobname") String jobname)throws IOException {
        return getQueryService().getCertainJobAllRunsId(cluster,user,jobname);
    }

    @GET
    @Path("job/{cluster}")
    @Produces(MediaType.APPLICATION_JSON)
    public JobDetails getJobById(@PathParam("cluster") String cluster,
                               @QueryParam("jobId") String jobId,
                               @DefaultValue("false")@QueryParam("counter") boolean counter ) throws IOException {
    LOG.info("Fetching JobDetails for jobId=" + jobId);
    Stopwatch timer = new Stopwatch().start();

    JobDetails jobDetails  = (jobId == null)?  null:
          getQueryService().getJobByJobID(cluster, jobId, counter, false);

    timer.stop();
    if (jobDetails != null) {
      LOG.info("For job/{cluster}/{jobId} with input query:" + " job/" + cluster + SLASH + jobId
          + " fetched jobDetails for " + jobDetails.getJobName() + " in " + timer);
    } else {
      LOG.info("For job/{cluster}/{jobId} with input query:" + " job/" + cluster + SLASH + jobId
          + " No jobDetails found, but spent " + timer);
    }
    return jobDetails;
    }


    @GET
    @Path("tasks/{cluster}/{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskDetails> getJobTasksById(@PathParam("cluster") String cluster,
                                           @PathParam("jobId") String jobId) throws IOException {
    LOG.info("Fetching tasks info for jobId=" + jobId);
    Stopwatch timer = new Stopwatch().start();
    JobDetails jobDetails = getQueryService().getJobByJobID(cluster, jobId, false, true);
    timer.stop();

    List<TaskDetails> tasks = jobDetails.getTasks();

    if(tasks != null && !tasks.isEmpty()) {
      LOG.info("For endpoint /tasks/" + cluster + "/" + jobId + ", fetched "
          + tasks.size() + " tasks, spent time " + timer);
    } else {
      LOG.info("For endpoint /tasks/" + cluster + "/" + jobId
          + ", found no tasks, spent time " + timer);
    }
    return tasks;
    }


    @GET
    @Path("running/status/{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public RunningStatusDAO getRunStatus(@PathParam("jobId") String jobId) throws IOException {
        LOG.debug("query running " +  jobId+" status");
        RunningStatusDAO dao = getQueryService().getRunningJobStatus(jobId);
        if (dao == null){
            return new RunningStatusDAO();
        }else {
            return dao;
        }
    }



    @GET
    @Path("running/id/{username}/{jobname}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> GetRunningJobID(@PathParam("username")String username,
                                        @PathParam("jobname")String jobname ) throws IOException {
        LOG.debug("query running " +  username+ "/" + jobname +"application id");
        return getQueryService().getRunningJobID(username,jobname);
    }

    @GET
    @Path("running/job/{username}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getRuningJobName(@PathParam("username") String username) throws IOException {
        LOG.debug("query running " +  username +" job name.");
        return getQueryService().getRunningJobName(username);
    }


    @GET
    @Path("alluser/{cluster}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, List<JobDetails>> scanAllUserUsage(
            @PathParam("cluster") String cluster,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time) throws IOException {

        List<String> users = getQueryService().getUserWithin2Mon();
        HashMap<String, List<JobDetails>> result = new HashMap<String, List<JobDetails>>();
        List<JobDetails> jobs;

        // return empty result when timestanp is not correct
        if(start_time < 0 || end_time < 0 || start_time > end_time ) {
            LOG.info(" timestanp is not correct, return empty result");
            return result;
        }
        for(String user:users) {
            LOG.info(String.format("Query [%s] SU",user));
            jobs = getQueryService().getAllJobInTimeInterval(cluster, user, start_time, end_time, false, Integer.MAX_VALUE);
            if(jobs.size() > 0)
                result.put(user,jobs);
        }
        return result;
    }



    /* post sample
    @POST
    @Path("running/id/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public List<String> getRunningJobID(UserJobDAO s ) throws IOException {
        LOG.debug("query running " +  s.getJobname()+ "/" + s.getUsername() +"application id");
        return getQueryService().getRunningJobID(s.getUsername(),s.getJobname());
    }
    */


}

