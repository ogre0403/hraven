package org.nchc.extend;

import com.twitter.hraven.Constants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendConstants extends Constants{
    //TODO: use control cahr instead of $, !
    public static final char SEP_CHAR2 = '$';
    public static final String SEP2 = "" + SEP_CHAR2;
    public static final byte[] SEP2_BYTES = Bytes.toBytes(SEP2);

    public static final char SEP_CHAR3 = 1;
    public static final String SEP3 = "" + SEP_CHAR3;
    public static final byte[] SEP3_BYTES = Bytes.toBytes(SEP3);

    public static final char SEP_CHAR4 = 2;
    public static final String SEP4 = "" + SEP_CHAR4;
    public static final byte[] SEP4_BYTES = Bytes.toBytes(SEP4);
    public static final String ROWKEY_BY_TS_COL = "rk_t";

    public static final char SEP_CHAR5 = 0;
    public static final String SEP5 = "" + SEP_CHAR5;
    public static final byte[] SEP5_BYTES = Bytes.toBytes(SEP5);

    public static final byte[] ROWKEY_BY_TS_COL_BYTES = Bytes.toBytes(ROWKEY_BY_TS_COL);
    public static final String MR_QUEUE =  "mapreduce.job.queuename";

    public static String RUNNING_JOB_TABLE = "job_running";
    public static byte[] RUNNING_JOB_TABLE_BYTES = Bytes.toBytes(RUNNING_JOB_TABLE);

    public static String RUNNING_JOB_CF = "r";
    public static byte[] RUNNING_JOB_CF_BYTES = Bytes.toBytes(RUNNING_JOB_CF);

    public static final String MAP_ATTEMPT_TYPE       = "m";
    public static final String REDUCE_ATTEMPT_TYPE    = "r";
    public static final String OTHER_ATTEMPT_TYPE     = "o";

    public static final String FINISH_TIME_COL = "finish_time";
    public static final byte[] FINISH_TIME_COL_BYTES = Bytes.toBytes(FINISH_TIME_COL);

    public static final String JOB_FINISHED_EVENT = "{\"type\":\"JOB_FINISHED";
    public static final byte[] JOB_FINISHED_EVENT_BYTES = Bytes.toBytes(JOB_FINISHED_EVENT);

    public static final String JOB_KILLED_EVENT = "{\"type\":\"JOB_KILLED";
    public static final byte[] JOB_KILLED_EVENT_BYTES = Bytes.toBytes(JOB_KILLED_EVENT);

    public static final String JOB_FAILED_EVENT = "{\"type\":\"JOB_FAILED";
    public static final byte[] JOB_FAILED_EVENT_BYTES = Bytes.toBytes(JOB_FAILED_EVENT);

    public static final String FINISHED_TIME_PREFIX_HADOOP2 = "\"finishTime\":";
    public static final byte[] FINISHED_TIME_PREFIX_HADOOP2_BYTES = Bytes.toBytes(FINISHED_TIME_PREFIX_HADOOP2);
}
