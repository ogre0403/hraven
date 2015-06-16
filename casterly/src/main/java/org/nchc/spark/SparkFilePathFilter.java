package org.nchc.spark;

import com.twitter.hraven.etl.JobFile;
import com.twitter.hraven.etl.JobFilePathFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by 1403035 on 2015/5/20.
 */
public class SparkFilePathFilter extends JobFilePathFilter {

    /**
     * The minimum modification time of a file to be accepted in milliseconds
     * since January 1, 1970 UTC (excluding).
     */
    private final long minModificationTimeMillis;

    /**
     * The maximum modification time of a file to be accepted in milliseconds
     * since January 1, 1970 UTC (including).
     */
    private final long maxModificationTimeMillis;


    /**
     * The configuration of this processing job (not the files we are processing).
     */
    private final Configuration myConf;
    private static Log LOG = LogFactory.getLog(SparkFilePathFilter.class);

    /**
     * Constructs a filter that accepts only JobFiles with lastModification time
     * in the specified range.
     *
     * @param myConf                    used to be able to go from a path to a FileStatus.
     * @param minModificationTimeMillis The minimum modification time of a file to be accepted in
     *                                  milliseconds since January 1, 1970 UTC (excluding).
     * @param maxModificationTimeMillis The
     *                                  maximum modification time of a file to be accepted in milliseconds
     *                                  since January 1, 1970 UTC (including).
     */
    public SparkFilePathFilter(Configuration myConf,
                               long minModificationTimeMillis) {
        this(myConf, minModificationTimeMillis, Long.MAX_VALUE);
    }


    public SparkFilePathFilter(Configuration myConf,
                                long minModificationTimeMillis,
                                long maxModificationTimeMillis) {
        this.myConf = myConf;
        this.minModificationTimeMillis = minModificationTimeMillis;
        this.maxModificationTimeMillis = maxModificationTimeMillis;
    }

    @Override
    public boolean accept(Path path) {
        if (!super.accept(path)) {
            return false;
        }

        JobFile jobFile = new JobFile(path);
        if (jobFile.isSparkFile()) {


            try {
                FileSystem fs = path.getFileSystem(myConf);
                FileStatus fileStatus = fs.getFileStatus(path);
                long fileModificationTimeMillis = fileStatus.getModificationTime();
                Path pp = new Path(path.getParent(),"APPLICATION_COMPLETE");
                return accept(fileModificationTimeMillis,pp) && fs.exists(pp);

            } catch (IOException e) {
                LOG.error("Can not check existence");
            }
            return true;
        } else {
            // Reject anything that does not match a job conf filename.
//            LOG.info(" Not a valid spark history file " + path.getName());
            return false;
        }
    }


    /**
     * @param fileModificationTimeMillis
     *          in milliseconds since January 1, 1970 UTC
     * @return whether a file with such modification time is to be accepted.
     */
    public boolean accept(long fileModificationTimeMillis, Path completeFile) {
        return ((minModificationTimeMillis < fileModificationTimeMillis) &&
                (fileModificationTimeMillis <= maxModificationTimeMillis));
    }


    /**
     * @return the minModificationTimeMillis used in for this filter.
     */
    public long getMinModificationTimeMillis() {
        return minModificationTimeMillis;
    }

    /**
     * @return the maxModificationTimeMillis used for this filter
     */
    public long getMaxModificationTimeMillis() {
        return maxModificationTimeMillis;
    }

}
