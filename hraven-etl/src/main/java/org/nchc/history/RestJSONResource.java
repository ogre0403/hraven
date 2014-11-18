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
package org.nchc.history;

import com.google.common.base.Stopwatch;
import com.twitter.hraven.*;
import com.twitter.hraven.datasource.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Main REST resource that handles binding the REST API to the JobHistoryService.
 *
 * TODO: better prevalidation
 * TODO: handle null results with empty json object or response code
 */
@Path("/api/v1/")
public class RestJSONResource {
  private static final Log LOG = LogFactory.getLog(RestJSONResource.class);
  private static final String SLASH = "/" ;

  private static final Configuration HBASE_CONF = HBaseConfiguration.create();

  private static final ThreadLocal<queryJobService> queryThreadLocal =
        new ThreadLocal<queryJobService>() {
            @Override
            protected queryJobService initialValue() {
                try {
                    LOG.info("Initializing queryJobService");
                    return new queryJobService(HBASE_CONF);
                } catch (IOException e) {
                    throw new RuntimeException("Could not initialize queryJobService", e);
                }
            }
  };

  private static final ThreadLocal<JobHistoryService> serviceThreadLocal =
    new ThreadLocal<JobHistoryService>() {
    @Override
    protected JobHistoryService initialValue() {
      try {
        LOG.info("Initializing JobHistoryService");
        return new JobHistoryService(HBASE_CONF);
      } catch (IOException e) {
        throw new RuntimeException("Could not initialize JobHistoryService", e);
      }
    }
  };

  private static final ThreadLocal<AppVersionService> serviceThreadLocalAppVersion =
      new ThreadLocal<AppVersionService>() {
      @Override
      protected AppVersionService initialValue() {
        try {
          LOG.info("Initializing AppVersionService");
          return new AppVersionService(HBASE_CONF);
        } catch (IOException e) {
          throw new RuntimeException("Could not initialize AppVersionService", e);
        }
      }
    };




    @GET
    @Path("job/{cluster}/{user}/{jobname}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JobDetails> getJobByTimeInterval(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @PathParam("jobname") String jobname,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time) throws IOException {
        if(start_time < 0 || end_time < 0 || start_time > end_time ) {
            // given incorrect Time interval or time interval not set, return all Job runs
            return getQueryService().getCertainJobAllRuns(cluster,user,jobname);
        }
        return getQueryService().getCertainJobRunsInTimeInterval(cluster,user,jobname,start_time,end_time);
    }


    @GET
    @Path("job/{cluster}/{user}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JobDetails> getJobByTimeInterval(
            @PathParam("cluster") String cluster,
            @PathParam("user") String user,
            @DefaultValue("-1")@QueryParam("start") long start_time,
            @DefaultValue("-1")@QueryParam("end") long end_time) throws IOException {
        if(start_time < 0 || end_time < 0 || start_time > end_time ) {
            // given incorrect Time interval or time interval not set, return all Job runs
            return getQueryService().getAllJobInTimeInterval(cluster,user);
        }
        return getQueryService().getAllJobInTimeInterval(cluster,user,start_time,end_time);
    }

  @GET
  @Path("job/{cluster}")
  @Produces(MediaType.APPLICATION_JSON)
  public JobDetails getJobById(@PathParam("cluster") String cluster,
                               @QueryParam("jobId") String jobId) throws IOException {
    LOG.info("Fetching JobDetails for jobId=" + jobId);
    Stopwatch timer = new Stopwatch().start();

      JobDetails jobDetails = getJobHistoryService().getJobByJobID(cluster, jobId);
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
    JobDetails jobDetails = getJobHistoryService().getJobByJobID(cluster, jobId, true);
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
   @Path("appVersion/{cluster}/{user}/{appId}/")
   @Produces(MediaType.APPLICATION_JSON)
   public List<VersionInfo> getDistinctVersions(@PathParam("cluster") String cluster,
                                    @PathParam("user") String user,
                                    @PathParam("appId") String appId,
                                    @QueryParam("limit") int limit) throws IOException {
     Stopwatch timer = new Stopwatch().start();

     if (LOG.isTraceEnabled()) {
      LOG.trace("Fetching App Versions for cluster=" + cluster + " user=" + user + " app=" + appId);
     }
     List<VersionInfo> distinctVersions = serviceThreadLocalAppVersion.get()
                                             .getDistinctVersions(
                                                 StringUtils.trimToEmpty(cluster),
                                                 StringUtils.trimToEmpty(user),
                                                 StringUtils.trimToEmpty(appId));
     timer.stop();

     LOG.info("For appVersion/{cluster}/{user}/{appId}/ with input query "
       + "appVersion/" + cluster + SLASH + user + SLASH + appId
       + "?limit=" + limit
       + " fetched #number of VersionInfo " + distinctVersions.size() + " in " );//+ timer);

     // export latency metrics
     return distinctVersions;
  }



    private static queryJobService getQueryService(){
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Returning JobHistoryService %s bound to thread %s",
                    queryThreadLocal.get(), Thread.currentThread().getName()));
        }
        return queryThreadLocal.get();
    }


  private static JobHistoryService getJobHistoryService() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returning JobHistoryService %s bound to thread %s",
        serviceThreadLocal.get(), Thread.currentThread().getName()));
    }
    return serviceThreadLocal.get();
  }
}
