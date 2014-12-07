package org.nchc.history;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.util.HadoopConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.nchc.extend.ExtendConstants;
import org.nchc.extend.ExtendJobKeyConverter;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobHistoryService extends JobHistoryService {
    public ExtendJobHistoryService(Configuration myConf) throws IOException {
        super(myConf);
    }


    public static List<Put> getHbasePuts(JobDesc jobDesc, Configuration jobConf) {
        List<Put> puts = new LinkedList<Put>();
        ExtendJobKeyConverter jkcvrt = new ExtendJobKeyConverter();

        JobKey jobKey = new JobKey(jobDesc);
        Put pp = jkcvrt.allJobRK(jobKey);
        puts.add(pp);

        byte[] jobKeyBytes = jkcvrt.toBytes(jobKey);
        byte[] jkByTS = jkcvrt.toBytesSortByTS(jobKey);

        // Add all columns to one put
        Put jobPut = new Put(jobKeyBytes);
        jobPut.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.VERSION_COLUMN_BYTES,
                Bytes.toBytes(jobDesc.getVersion()));
        jobPut.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.FRAMEWORK_COLUMN_BYTES,
                Bytes.toBytes(jobDesc.getFramework().toString()));

        Put jobputbyTS = new Put(jkByTS);
        jobputbyTS.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.VERSION_COLUMN_BYTES,
                Bytes.toBytes(jobDesc.getVersion()));
        jobputbyTS.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.FRAMEWORK_COLUMN_BYTES,
                Bytes.toBytes(jobDesc.getFramework().toString()));

        // Avoid doing string to byte conversion inside loop.
        byte[] jobConfColumnPrefix = Bytes.toBytes(ExtendConstants.JOB_CONF_COLUMN_PREFIX
                + ExtendConstants.SEP);

        // Create puts for all the parameters in the job configuration
        Iterator<Map.Entry<String, String>> jobConfIterator = jobConf.iterator();
        while (jobConfIterator.hasNext()) {
            Map.Entry<String, String> entry = jobConfIterator.next();
            // Prefix the job conf entry column with an indicator to
            byte[] column = Bytes.add(jobConfColumnPrefix,
                    Bytes.toBytes(entry.getKey()));
            jobPut.add(ExtendConstants.INFO_FAM_BYTES, column, Bytes.toBytes(entry.getValue()));
            jobputbyTS.add(ExtendConstants.INFO_FAM_BYTES, column, Bytes.toBytes(entry.getValue()));
        }

        // ensure pool/queuename is set correctly
        setHravenQueueNamePut(jobConf, jobPut, jobKey, jobConfColumnPrefix);
        setHravenQueueNamePut(jobConf, jobputbyTS, jobKey, jobConfColumnPrefix);

        puts.add(jobPut);
        puts.add(jobputbyTS);
        return puts;
    }

    static void setHravenQueueNamePut(Configuration jobConf, Put jobPut,
                                      JobKey jobKey, byte[] jobConfColumnPrefix) {

        String hRavenQueueName = HadoopConfUtil.getQueueName(jobConf);
        if (hRavenQueueName.equalsIgnoreCase(Constants.DEFAULT_VALUE_QUEUENAME)){
            // due to a bug in hadoop2, the queue name value is the string "default"
            // hence set it to username
            hRavenQueueName = jobKey.getUserName();
        }

        // set the "queue" property defined by hRaven
        // this makes it independent of hadoop version config parameters
        byte[] column = Bytes.add(jobConfColumnPrefix, Constants.HRAVEN_QUEUE_BYTES);
        jobPut.add(Constants.INFO_FAM_BYTES, column,
                Bytes.toBytes(hRavenQueueName));
    }
}
