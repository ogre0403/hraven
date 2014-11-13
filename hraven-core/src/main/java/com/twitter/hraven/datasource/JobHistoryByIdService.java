/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.hraven.datasource;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;

/**
 * Service to access the {@link Constants#HISTORY_BY_JOBID_TABLE}.
 * 
 */
public class JobHistoryByIdService {
  private JobKeyConverter jobKeyConv = new JobKeyConverter();
  private QualifiedJobIdConverter jobIdConv = new QualifiedJobIdConverter();

  /**
   * Used to store the job to jobHistoryKey index in.
   */
  private final HTable historyByJobIdTable;

  public JobHistoryByIdService(Configuration myHBaseConf) throws IOException {
    historyByJobIdTable = new HTable(myHBaseConf,
        Constants.HISTORY_BY_JOBID_TABLE_BYTES);
  }

  /**
   * Release internal HBase table instances. Must be called when consumer is
   * done with this service.
   * 
   * @throws IOException
   *           when bad things happen closing HBase table(s).
   */
  public void close() throws IOException {
    if (historyByJobIdTable != null) {
      historyByJobIdTable.close();
    }
  }

  /**
   * Returns the JobKey for the job_history table, stored for this job ID,
   * or {@code null} if not found.
   * @param jobId the cluster and job ID combination to look up
   * @return the JobKey instance stored, or {@code null} if not found
   * @throws IOException if thrown by the HBase client
   */
  public JobKey getJobKeyById(QualifiedJobId jobId) throws IOException {
    byte[] indexKey = jobIdConv.toBytes(jobId);

    Get g = new Get(indexKey);
    g.addColumn(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES);
    Result r = historyByJobIdTable.get(g);
    if (r != null && !r.isEmpty()) {
      byte[] historyKey = r.getValue(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES);
      if (historyKey != null && historyKey.length > 0) {
        return jobKeyConv.fromBytes(historyKey);
      }
    }
    return null;
  }

  /**
   * Create the secondary indexes records cluster!jobId->jobKey.
   * 
   * @param jobKey
   * @throws IOException
   *           if the entry cannot be written.
   */
  public void writeIndexes(JobKey jobKey) throws IOException {
    // Defensive coding
    if (jobKey != null) {
      byte[] jobKeyBytes = jobKeyConv.toBytes(jobKey);
      byte[] jobKeyTsBytes = jobKeyConv.toBytesSortByTS(jobKey);
      byte[] rowKeyBytes = jobIdConv.toBytes(
          new QualifiedJobId(jobKey.getCluster(), jobKey.getJobId()) );

      // Insert (or update) row with jobid as the key
      Put p = new Put(rowKeyBytes);
      p.add(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES, jobKeyBytes);
      p.add(Constants.INFO_FAM_BYTES, Constants.ROWKEY_BY_TS_COL_BYTES, jobKeyTsBytes);
      historyByJobIdTable.put(p);
    }
  }

}
