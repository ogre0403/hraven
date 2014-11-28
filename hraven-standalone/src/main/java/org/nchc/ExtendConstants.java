package org.nchc;

import com.twitter.hraven.Constants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendConstants extends Constants{
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

    public static final byte[] ROWKEY_BY_TS_COL_BYTES = Bytes.toBytes(ROWKEY_BY_TS_COL);
    public static final String MR_QUEUE =  "mapreduce.job.queuename";

    public static final String MAP_ATTEMPT_TYPE       = "m";
    public static final String REDUCE_ATTEMPT_TYPE    = "r";
    public static final String OTHER_ATTEMPT_TYPE     = "o";
}
