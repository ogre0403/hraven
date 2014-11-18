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

import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobId;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.util.ByteUtil;

/**
 */
public class JobKeyConverter implements ByteConverter<JobKey> {
  private JobIdConverter idConv = new JobIdConverter();

  /**
   * Returns the byte encoded representation of a JobKey
   *
   * @param jobKey the JobKey to serialize
   * @return the byte encoded representation of the JobKey
   */
  @Override
  public byte[] toBytes(JobKey jobKey) {
    if (jobKey == null) {
      return Constants.EMPTY_BYTES;
    } else {
      return ByteUtil.join(Constants.SEP_BYTES,
          Bytes.toBytes(jobKey.getCluster()),
          Bytes.toBytes(jobKey.getUserName()),
          Bytes.toBytes(jobKey.getAppId()),
          Bytes.toBytes(jobKey.getEncodedRunId()),
          idConv.toBytes(jobKey.getJobId()));
    }
  }

    /**
     * cluster!user $ runID!JobName!JobID
     *
     * */
    public byte[] toBytesSortByTS(JobKey jobKey) {
        if (jobKey == null) {
            return Constants.EMPTY_BYTES;
        } else {
            return Bytes.add(
                    Bytes.add(Bytes.toBytes(jobKey.getCluster()),   // cluster
                            Constants.SEP_BYTES,                    //!
                            Bytes.toBytes(jobKey.getUserName())),   //user
                    Constants.SEP2_BYTES,                           //$
                    ByteUtil.join(Constants.SEP_BYTES,              //!
                            Bytes.toBytes(jobKey.getEncodedRunId()),//runID
                            Bytes.toBytes(jobKey.getAppId()),       //JobName
                            idConv.toBytes(jobKey.getJobId())       //JobID
                            )
            );
        }
    }


  /**
   * Reverse operation of
   * {@link JobKeyConverter#toBytes(com.twitter.hraven.JobKey)}
   *
   * @param bytes the serialized version of a JobKey
   * @return a deserialized JobKey instance
   */
  @Override
  public JobKey fromBytes(byte[] bytes) {
    byte[][] splits = splitJobKey(bytes);
    // no combined runId + jobId, parse as is
    return parseJobKey(splits);
  }

  public JobKey fromTsSortedBytes(byte[] bytes){
      byte[][] splits = splitTsSortedJobKey(bytes);
      return parseJobKey(splits);
  }

  public byte[][] splitTsSortedJobKey(byte[] rawKey) {
      byte[][] outarray = new byte[5][];
      byte[][] splits = ByteUtil.split(rawKey, Constants.SEP2_BYTES, 2);
      byte[][] split2 = ByteUtil.split(splits[0],Constants.SEP_BYTES,2);
      byte[][] split3 = ByteUtil.split(splits[1],Constants.SEP_BYTES,3);


      outarray[0] = split2[0];
      outarray[1] = split2[1];
      outarray[2] = split3[1];
      outarray[3] = split3[0];
      outarray[4] = split3[2];

      return outarray;
  }

  /**
   * Constructs a JobKey instance from the individual byte encoded key
   * components.
   *
   * @param keyComponents
   *          as split on
   * @return a JobKey instance containing the decoded components
   */
  public JobKey parseJobKey(byte[][] keyComponents) {
    // runId is inverted in the bytes representation so we get reverse
    // chronological order
    long encodedRunId = keyComponents.length > 3 ?
        Bytes.toLong(keyComponents[3]) : Long.MAX_VALUE;

    JobId jobId = keyComponents.length > 4 ?
        idConv.fromBytes(keyComponents[4]) : null;

    return new JobKey(Bytes.toString(keyComponents[0]),
        (keyComponents.length > 1 ? Bytes.toString(keyComponents[1]) : null),
        (keyComponents.length > 2 ? Bytes.toString(keyComponents[2]) : null),
        Long.MAX_VALUE - encodedRunId,
        jobId);
  }

  /**
   * Handles splitting the encoded job key correctly, accounting for long
   * encoding of the run ID.  Since the long encoding of the run ID may
   * legitimately contain the separator bytes, we first split the leading 3
   * elements (cluster!user!appId), then split out the runId and remaining
   * fields based on the encoded long length;
   *
   * @param rawKey byte encoded representation of the job key
   * @return
   */
  static byte[][] splitJobKey(byte[] rawKey) {
    byte[][] splits = ByteUtil.split(rawKey, Constants.SEP_BYTES, 4);

    /* final components (runId!jobId!additional) need to be split separately for correct
     * handling of runId long encoding */
    if (splits.length == 4) {
      // TODO: this splitting is getting really ugly, look at using Orderly instead for keying
      byte[] remainder = splits[3];
      byte[][] extraComponents = new byte[3][];

      int offset = 0;
      // run ID
      extraComponents[0] = ByteUtil.safeCopy(remainder, offset, 8);
      // followed by sep + job epoch + job seq
      offset += 8+Constants.SEP_BYTES.length;
      extraComponents[1] = ByteUtil.safeCopy(remainder, offset, 16);
      offset += 16+Constants.SEP_BYTES.length;
      // followed by any remainder
      extraComponents[2] = ByteUtil.safeCopy(remainder, offset, remainder.length - offset);

      int extraSize = 0;
      // figure out the full size of all splits
      for (int i=0; i < extraComponents.length; i++) {
        if (extraComponents[i] != null) {
          extraSize++;
        } else {
          break; // first null signals hitting the end of remainder
        }
      }

      byte[][] allComponents = new byte[3+extraSize][];
      // fill in the first 3 elts
      for (int i=0; i < 3; i++) {
        allComponents[i] = splits[i];
      }
      // add any extra that were non-null
      for (int i=0; i < extraSize; i++) {
        allComponents[3+i] = extraComponents[i];
      }

      return allComponents;
    }
    return splits;
  }
}
