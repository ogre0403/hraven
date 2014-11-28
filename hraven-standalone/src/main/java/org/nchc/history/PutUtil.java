package org.nchc.history;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.etl.JobHistoryFileParserBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.nchc.ExtendJobKeyConverter;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by 1403035 on 2014/11/11.
 */
public class PutUtil {
    private static ExtendJobKeyConverter jobKeyConv = new ExtendJobKeyConverter();
    private static Log LOG = LogFactory.getLog(PutUtil.class);

    public static List<Put> getJobCostPut(Double jobCost, JobKey jobKey) {

        List<Put> ps = new LinkedList<Put>();
        Put pJobCost = new Put(jobKeyConv.toBytes(jobKey));
        pJobCost.add(Constants.INFO_FAM_BYTES, Constants.JOBCOST_BYTES, Bytes.toBytes(jobCost));
        ps.add(pJobCost);
        pJobCost = new Put(jobKeyConv.toBytesSortByTS(jobKey));
        pJobCost.add(Constants.INFO_FAM_BYTES, Constants.JOBCOST_BYTES, Bytes.toBytes(jobCost));
        ps.add(pJobCost);
        return ps;
    }

    public static List<Put> getMegaByteMillisPut(Long mbMillis, JobKey jobKey) {
        List<Put> ps = new LinkedList<Put>();
        Put pMb = new Put(jobKeyConv.toBytes(jobKey));
        pMb.add(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(mbMillis));
        ps.add(pMb);
        pMb = new Put(jobKeyConv.toBytesSortByTS(jobKey));
        pMb.add(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(mbMillis));
        ps.add(pMb);
        return ps;
    }

    public static void addFileNamePut(List<Put> puts, byte[] rowKey,
                                byte[] filenameColumn, String filename) {
        Put put = new Put(rowKey);
        put.add(Constants.INFO_FAM_BYTES, filenameColumn, Bytes.toBytes(filename));
        puts.add(put);
    }

    public static void addRawPut(List<Put> puts, byte[] rowKey, byte[] rawColumn,
                           byte[] lastModificationColumn, FileStatus fileStatus, FileSystem hdfs) throws IOException {
        byte[] rawBytes = readJobFile(fileStatus,hdfs);

        Put raw = new Put(rowKey);

        byte[] rawLastModifiedMillis = Bytes.toBytes(fileStatus
                .getModificationTime());

        raw.add(Constants.RAW_FAM_BYTES, rawColumn, rawBytes);
        raw.add(Constants.INFO_FAM_BYTES, lastModificationColumn,
                rawLastModifiedMillis);
        puts.add(raw);
    }

    private static byte[] readJobFile(FileStatus fileStatus, FileSystem hdfs) throws IOException {
        byte[] rawBytes = null;
        FSDataInputStream fsdis = null;
        try {
            long fileLength = fileStatus.getLen();
            int fileLengthInt = (int) fileLength;
            rawBytes = new byte[fileLengthInt];
            fsdis = hdfs.open(fileStatus.getPath());
            IOUtils.readFully(fsdis, rawBytes, 0, fileLengthInt);
        } finally {
            IOUtils.closeStream(fsdis);
        }
        return rawBytes;
    }

    public static Double getJobCost(Long mbMillis, String machineType, String costdetail) {
        Double computeTco = 0.0;
        Long machineMemory = 0L;
        Properties prop = null;
        LOG.debug(" machine type " + machineType);
        prop = loadCostProperties(costdetail);


        if (prop != null) {
            String computeTcoStr = prop.getProperty(machineType + ".computecost");
            try {
                computeTco = Double.parseDouble(computeTcoStr);
            } catch (NumberFormatException nfe) {
                LOG.error("error in conversion to long for compute tco " + computeTcoStr
                        + " using default value of 0");
            }
            String machineMemStr = prop.getProperty(machineType + ".machinememory");
            try {
                machineMemory = Long.parseLong(machineMemStr);
            } catch (NumberFormatException nfe) {
                LOG.error("error in conversion to long for machine memory  " + machineMemStr
                        + " using default value of 0");
            }
        } else {
            LOG.error("Could not load properties file, using defaults");
        }

        Double jobCost = JobHistoryFileParserBase.calculateJobCost(mbMillis, computeTco, machineMemory);
        LOG.info("from cost properties file, jobCost is " + jobCost + " based on compute tco: "
                + computeTco + " machine memory: " + machineMemory + " for machine type " + machineType);
        return jobCost;
    }


    private static Properties loadCostProperties(String costDetailsPath) {
        Properties prop = new Properties();
        InputStream inp = PutUtil.class.getClassLoader().getResourceAsStream(costDetailsPath);

        try {
            if (inp == null){
                return null;
            }
            prop.load(inp);
            return prop;
        }catch (IOException e) {
            return null;
        } finally {
            if (inp != null) {
                try {
                    inp.close();
                } catch (IOException ignore) {
                    // do nothing
                }
            }
        }
    }

}
