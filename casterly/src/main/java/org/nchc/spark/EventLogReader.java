package org.nchc.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.nchc.spark.dao.AppEnd;
import org.nchc.spark.dao.AppStart;
import org.nchc.spark.dao.SparkDetails;

import java.io.IOException;

/**
 * Created by 1403035 on 2015/6/9.
 */
public class EventLogReader {
    static Log LOG = LogFactory.getLog(EventLogReader.class);
    private Configuration conf;
    private FileSystem hdfs;
    private Gson gson;

    public EventLogReader() throws IOException {
        conf = new Configuration();
        hdfs = FileSystem.get(conf);
        gson = new GsonBuilder().create();
    }


    public SparkDetails read(String spraklogiflepath) throws IOException {
        LOG.info("read "+ spraklogiflepath);
        SparkDetails details = new SparkDetails();
        Path path = new Path(spraklogiflepath);

        FSDataInputStream in = hdfs.open(path);


        // skip first  lines
        in.readLine();

        // get executoru count from SparkListenerEnvironmentUpdate
        JSONObject jsonObject = new JSONObject(in.readLine());
        String s = jsonObject.getJSONObject("Spark Properties").getString("spark.executor.instances");
        details.setExecutorCount(Integer.parseInt(s) + 1);

        // AppStart
        AppStart as = gson.fromJson(in.readLine(),AppStart.class);
        if(as != null) {
            details.setUser(as.getUser());
            details.setAppName(as.getApp_Name());
            details.setAppID(as.getApp_ID());
            details.setStartTimestamp(as.getTimestamp());
        }


        // AppEnd
        long size = getflSize(path);
        in.seek(size - 100);
        in.readLine();
        AppEnd ae = gson.fromJson(in.readLine(), AppEnd.class);
        if (ae !=null) {
            details.setEndTimestamp(ae.getTimestamp());
        }else {
            details.setEndTimestamp(getflModTime(path));
            details.setJobFailed();
        }
        in.close();
        return details;
    }


    public boolean delete(String spraklogiflepath) throws IOException {
        Path path = new Path(spraklogiflepath).getParent();
        return hdfs.delete(path,true);
    }


    private long getflSize(Path path) throws IOException {
        ContentSummary cSummary = hdfs.getContentSummary(path);
        long length = cSummary.getLength();
        return length;
    }

    private long getflModTime(Path path) throws IOException {
        return hdfs.getFileStatus(path).getModificationTime();
    }
}
