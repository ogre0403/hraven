package org.nchc.extend;

import com.twitter.hraven.JobId;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.util.ByteUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.nchc.extend.ExtendConstants;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobKeyConverter extends JobKeyConverter{
   private static final Log LOG = LogFactory.getLog(ExtendJobKeyConverter.class);
   public Put allJobRK(JobKey jobKey){
       // cluster ! user (SOH) JobName
       JobId jid = jobKey.getJobId();

       byte[] clusert_user_bytes =
               Bytes.add(Bytes.toBytes(jobKey.getCluster()),
                       ExtendConstants.SEP_BYTES,
                       Bytes.toBytes(jobKey.getUserName()));
       Put p = new Put(Bytes.add(clusert_user_bytes,
                        ExtendConstants.SEP3_BYTES,
                        Bytes.toBytes(jobKey.getAppId()))
       );

       if (jobKey == null) {
               return null;
           }else{
                 p.add(ExtendConstants.INFO_FAM_BYTES,Bytes.toBytes(jid.getJobIdString()),ExtendConstants.EMPTY_BYTES);
           }
       return p;
   }


    public byte[] toBytesSortByTS(JobKey jobKey) {
        if (jobKey == null) {
            return ExtendConstants.EMPTY_BYTES;
        } else {
            return Bytes.add(
                    Bytes.add(Bytes.toBytes(jobKey.getCluster()),   // cluster
                            ExtendConstants.SEP_BYTES,                    //!
                            Bytes.toBytes(jobKey.getUserName())),   //user
                    ExtendConstants.SEP2_BYTES,                           //$
                    ByteUtil.join(ExtendConstants.SEP_BYTES,              //!
                            Bytes.toBytes(jobKey.getEncodedRunId()),//runID
                            Bytes.toBytes(jobKey.getAppId()),       //JobName
                            idConv.toBytes(jobKey.getJobId())       //JobID
                    )
            );
        }
    }


    public JobKey fromTsSortedBytes(byte[] bytes){
        byte[][] splits = splitTsSortedJobKey(bytes);
        return parseJobKey(splits);
    }


    //cluster!user $ runid!jobname!job_epoch job_seq
    /**
     * Since the long encoding of the run ID may legitimately contain the
     * separator bytes, we first split the leading elements (cluster!user)
     * and remaonder (runid!jobname!job_epoch job_seq) by dollar sign ($).
     * Then split out the runId, jobname, job_epoch, job_seq by encoded long length.
     */
    public byte[][] splitTsSortedJobKey(byte[] rawKey){
        byte[][] outarray = new byte[5][];
        byte[][] splits = ByteUtil.split(rawKey, ExtendConstants.SEP2_BYTES, 2);
        byte[][] prefix = ByteUtil.split(splits[0],ExtendConstants.SEP_BYTES,2);

        byte[] remainder = splits[1];

        for (int i=0; i < 1; i++) {
            outarray[i] = prefix[i];
        }
        outarray[3] = ByteUtil.safeCopy(remainder, 0, 8);
        outarray[4] = ByteUtil.safeCopy(remainder, remainder.length-16, 16);
        int len = remainder.length - 8-8-8-1-1;
        outarray[2] = ByteUtil.safeCopy(remainder, 9, len);

        return outarray;
    }

}
