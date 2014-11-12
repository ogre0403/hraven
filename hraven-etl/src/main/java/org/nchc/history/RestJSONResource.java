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

  private static final ThreadLocal<AppSummaryService> serviceThreadLocalAppService =
        new ThreadLocal<AppSummaryService>() {

        @Override
        protected AppSummaryService initialValue() {
          try {
            LOG.info("Initializing AppService");
            return new AppSummaryService(HBASE_CONF);
          } catch (IOException e) {
            throw new RuntimeException("Could not initialize AppService", e);
          }
        }
      };

  @GET
  @Path("job/{cluster}/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public JobDetails getJobById(@PathParam("cluster") String cluster,
                               @PathParam("jobId") String jobId) throws IOException {
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
    // export latency metrics
    HravenResponseMetrics.JOB_API_LATENCY_VALUE.set(timer.elapsed(TimeUnit.MILLISECONDS));

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
     HravenResponseMetrics.APPVERSIONS_API_LATENCY_VALUE
       .set(timer.elapsed(TimeUnit.MILLISECONDS));
     return distinctVersions;
  }



  @GET
  @Path("newJobs/{cluster}/")
  @Produces(MediaType.APPLICATION_JSON)
  public List<AppSummary> getNewJobs(@PathParam("cluster") String cluster,
                                   @QueryParam("user") String user,
                                   @QueryParam("startTime") long startTime,
                                   @QueryParam("endTime") long endTime,
                                   @QueryParam("limit") int limit)
                                       throws IOException {
    Stopwatch timer = new Stopwatch().start();

    if(limit == 0) {
      limit = Integer.MAX_VALUE;
    }
    if(startTime == 0L) {
      // 24 hours back
      startTime = System.currentTimeMillis() - Constants.MILLIS_ONE_DAY ;
      // get top of the hour
      startTime -= (startTime % 3600);
    }
    if(endTime == 0L) {
      // now
      endTime = System.currentTimeMillis() ;
      // get top of the hour
      endTime -= (endTime % 3600);
    }

    LOG.info("Fetching new Jobs for cluster=" + cluster + " user=" + user
       + " startTime=" + startTime + " endTime=" + endTime);
    AppSummaryService as = getAppSummaryService();
    // get the row keys from AppVersions table via JobHistoryService
    List<AppSummary> newApps = as.getNewApps(getJobHistoryService(),
          StringUtils.trimToEmpty(cluster), StringUtils.trimToEmpty(user),
          startTime, endTime, limit);

    timer.stop();

    LOG.info("For newJobs/{cluster}/{user}/{appId}/ with input query "
      + "newJobs/" + cluster + SLASH + user
      + "?limit=" + limit
      + "&startTime=" + startTime
      + "&endTime=" + endTime
      + " fetched " + newApps.size() + " flows in " + timer);

   // export latency metrics
   HravenResponseMetrics.NEW_JOBS_API_LATENCY_VALUE
       .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return newApps;
 }

  private AppSummaryService getAppSummaryService() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returning AppService %s bound to thread %s",
        serviceThreadLocalAppService.get(), Thread.currentThread().getName()));
    }
    return serviceThreadLocalAppService.get();
  }


  private static JobHistoryService getJobHistoryService() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returning JobHistoryService %s bound to thread %s",
        serviceThreadLocal.get(), Thread.currentThread().getName()));
    }
    return serviceThreadLocal.get();
  }
}
