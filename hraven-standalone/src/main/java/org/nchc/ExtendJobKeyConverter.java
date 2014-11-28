package org.nchc;

import com.twitter.hraven.JobId;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.util.ByteUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobKeyConverter extends JobKeyConverter{

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


    public byte[][] splitTsSortedJobKey(byte[] rawKey) {
        byte[][] outarray = new byte[5][];
        byte[][] splits = ByteUtil.split(rawKey, ExtendConstants.SEP2_BYTES, 2);
        byte[][] split2 = ByteUtil.split(splits[0],ExtendConstants.SEP_BYTES,2);
        byte[][] split3 = ByteUtil.split(splits[1],ExtendConstants.SEP_BYTES,3);

        outarray[0] = split2[0];
        outarray[1] = split2[1];
        outarray[2] = split3[1];
        outarray[3] = split3[0];
        outarray[4] = split3[2];

        return outarray;
    }

}
