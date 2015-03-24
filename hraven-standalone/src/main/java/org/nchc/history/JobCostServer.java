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
    package org.nchc.history;

    import java.io.IOException;
    import java.io.InterruptedIOException;
    import java.io.PrintWriter;
    import java.io.StringWriter;
    import java.util.*;

    import com.twitter.hraven.*;
    import com.twitter.hraven.datasource.*;
    import com.twitter.hraven.etl.*;
    import com.twitter.hraven.util.BatchUtil;
    import org.apache.commons.cli.CommandLine;
    import org.apache.commons.cli.CommandLineParser;
    import org.apache.commons.cli.HelpFormatter;
    import org.apache.commons.cli.Option;
    import org.apache.commons.cli.Options;
    import org.apache.commons.cli.ParseException;
    import org.apache.commons.cli.PosixParser;
    import org.apache.commons.logging.Log;
    import org.apache.commons.logging.LogFactory;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.*;
    import org.apache.hadoop.hbase.HBaseConfiguration;
    import org.apache.hadoop.hbase.KeyValue;
    import org.apache.hadoop.hbase.client.*;
    import org.apache.hadoop.hbase.util.Bytes;
    import org.apache.hadoop.util.GenericOptionsParser;
    import org.apache.log4j.Level;
    import org.apache.log4j.Logger;
    import org.nchc.extend.*;

    /**
     * Command line tool that can be run on a periodic basis (like daily, hourly, or
     * every 15 minutes or 1/2 hour). Each run is recorded by inserting a new
     * {@link com.twitter.hraven.etl.ProcessRecord} in {@link com.twitter.hraven.etl.ProcessState#CREATED} state. When the total
     * processing completes successfully, then the record state will be updated to
     * {@link com.twitter.hraven.etl.ProcessState#PREPROCESSED} to indicate that this batch has been
     * successfully updated. The run start time will be recorded in as
     * {@link com.twitter.hraven.etl.ProcessRecord#getMaxModificationTimeMillis()} so it can be used as the
     * starting mark for the next run if the previous run is successful
     */
    public class JobCostServer extends Thread {

      public final static String NAME = JobCostServer.class.getSimpleName();
      private static Log LOG = LogFactory.getLog(JobCostServer.class);

      /**
       * Maximum number of files to process in one batch.
       */
      private final static int DEFAULT_BATCH_SIZE = 1000;

      /**
       * Maximum size of file that be loaded into raw table : 500 MB
       */
      private final static long DEFAULT_RAW_FILE_SIZE_LIMIT = 524288000;

      private long keyCount = 0;
      private Configuration hbaseConf;
      private FileSystem hdfs;
      private ExtendJobHistoryRawService rawService = null;
      private ExtendJobHistoryByIdService jobHistoryByIdService = null;
      private ProcessRecordService processRecordService = null;
      private HTable rawTable = null;
      private HTable jobTable = null;
      private HTable taskTable = null;
      private HTable runningTable = null;
      private HashMap<JobFile,FileStatus> jobstatusmap = null;
      private static String macType = "default";

      private static String cluster;
      private static Path inputPath;
      private static int batchSize = DEFAULT_BATCH_SIZE;
      private static long maxFileSize = DEFAULT_RAW_FILE_SIZE_LIMIT;

      private static int INTERVAL=30000;
      private static Properties prop;
      private boolean isRunning = true;

      public JobCostServer(Properties ps ){
          this.prop = ps;
          this.cluster =ps.getProperty("history.cluster","nchc");
          this.inputPath= new Path(ps.getProperty("history.hdfsdir"));
          this.INTERVAL = Integer.parseInt(ps.getProperty("history.interval","40000"));
          this.macType = ps.getProperty("machinetype","default");
      }

      /**
       * Parse command-line arguments.
       *
       * @param args
       *          command line arguments passed to program.
       * @return parsed command line.
       * @throws org.apache.commons.cli.ParseException
       */
      private CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();

        // Cluster
        Option o = new Option("c", "cluster", true,
            "cluster for which jobs are processed");
        o.setArgName("cluster");
        o.setRequired(false);
        options.addOption(o);


        // Input
        o = new Option("i", "input", true,
            "input directory in hdfs. Default is mapreduce.jobtracker.jobhistory.completed.location.");
        o.setArgName("input-path");
        o.setRequired(false);
        options.addOption(o);


          // machine type
         o = new Option("t",  "type",  true,
                  "machine type in cost file. Default is default");
         o.setArgName("machine-type");
         o.setRequired(false);
         options.addOption(o);

        // Batch
        o = new Option("b", "batchSize", true,
            "The number of files to process in one batch. Default "
                + DEFAULT_BATCH_SIZE);
        o.setArgName("batch-size");
        o.setRequired(false);
        options.addOption(o);

        // raw file size limit
        o = new Option("s", "rawFileSize", true,
            "The max size of file that can be loaded into raw table. Default "
                + DEFAULT_RAW_FILE_SIZE_LIMIT);
        o.setArgName("rawfile-size");
        o.setRequired(false);
        options.addOption(o);


        // Debugging
        options.addOption("d", "debug", false, "switch on DEBUG log level");

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
        try {
          commandLine = parser.parse(options, args);
        } catch (Exception e) {
          LOG.error("ERROR: " + e.getMessage() + "\n");
          HelpFormatter formatter = new HelpFormatter();
          formatter.printHelp(NAME + " ", options, true);
          System.exit(-1);
        }

        // Set debug level right away
        if (commandLine.hasOption("d")) {
          Logger log = Logger.getLogger(JobFileRawLoader.class);
          log.setLevel(Level.DEBUG);
        }

        return commandLine;
      }

      public void getArgValue(String[] args) throws Exception{
          // Grab input args and allow for -Dxyz style arguments
//          hbaseConf = HBaseConfiguration.create(getConf());
          hbaseConf = HBaseConfiguration.create();
          hbaseConf.setInt("hbase.client.keyvalue.maxsize", 0);

          String[] otherArgs = new GenericOptionsParser(hbaseConf, args).getRemainingArgs();

          // Grab the arguments we're looking for.
          CommandLine commandLine = parseArgs(otherArgs);
          // Grab the cluster argument from cmd
          if (commandLine.hasOption("c")) {
              cluster = commandLine.getOptionValue("c");
          }
          hbaseConf.setStrings(Constants.CLUSTER_JOB_CONF_KEY, cluster);
          LOG.info("cluster: " + cluster);

          hdfs = FileSystem.get(hbaseConf);
          rawService = new ExtendJobHistoryRawService(hbaseConf);
          jobHistoryByIdService = new ExtendJobHistoryByIdService(hbaseConf);
          processRecordService = new ProcessRecordService(hbaseConf);
          rawTable = new HTable(hbaseConf, Constants.HISTORY_RAW_TABLE_BYTES);
          jobTable = new HTable(hbaseConf,Constants.HISTORY_TABLE_BYTES);
          taskTable = new HTable(hbaseConf,Constants.HISTORY_TASK_TABLE_BYTES);
          runningTable = new HTable(hbaseConf, ExtendConstants.RUNNING_JOB_TABLE);
          jobstatusmap = new HashMap<JobFile, FileStatus>();


          // Grab the input path argument
          String input;
          if (commandLine.hasOption("i")) {
              input = commandLine.getOptionValue("i");
              inputPath = new Path(input);
              FileStatus inputFileStatus = hdfs.getFileStatus(inputPath);
              if (!inputFileStatus.isDir()) {
                  throw new IOException("Input is not a directory"
                          + inputFileStatus.getPath().getName());
              }
          }
          LOG.info("history dir: " + inputPath.toString());

          // Grab the batch-size argument
          if (commandLine.hasOption("b")) {
              try {
                  batchSize = Integer.parseInt(commandLine.getOptionValue("b"));
              } catch (NumberFormatException nfe) {
                  throw new IllegalArgumentException(
                          "batch size option -b is is not a valid number: "
                                  + commandLine.getOptionValue("b"), nfe);
              }
              // Additional check
              if (batchSize < 1) {
                  throw new IllegalArgumentException(
                          "Cannot process files in batches smaller than 1. Specified batch size option -b is: "
                                  + commandLine.getOptionValue("b"));
              }
          }
          LOG.info("batchsize=" + batchSize);

          // raw file size limit
          if (commandLine.hasOption("s")) {
              String maxFileSizeStr = commandLine.getOptionValue("s");
              try {
                  maxFileSize = Long.parseLong(maxFileSizeStr);
              } catch (NumberFormatException nfe) {
                  throw new ProcessingException("Caught NumberFormatException during conversion "
                          + " of maxFileSize to long", nfe);
              }
          }
          LOG.info("maxFileSize=" + maxFileSize);

          if (commandLine.hasOption("t")) {
              macType = commandLine.getOptionValue("t");
          }
          LOG.info("machine type: " + macType);
          LOG.info("history.interval: " + INTERVAL);
      }


      public void run(){
          LOG.info("start separate thread to parse history file");
          while (isRunning) {
            try {

              FileStatus[] jobFileStatusses;
              jobFileStatusses = findProcessFiles();
              if (jobFileStatusses.length < 1) {
                  Thread.sleep(INTERVAL);
                  continue;
              }
              LOG.info("Found newly job, start parse");
              run1(jobFileStatusses);
            }catch(InterruptedException e)
            {
                LOG.info("Thread was inturrupted");
            }catch (Exception e) {
              StringWriter errors = new StringWriter();
              e.printStackTrace(new PrintWriter(errors));
              LOG.error(errors.toString());
            }
          }
      }

      private void run1(FileStatus[] jobFileStatusses) throws Exception {
            LOG.debug("NEW history parse iteration....");
            LOG.info("Sorting " + jobFileStatusses.length + " job files.");
            Arrays.sort(jobFileStatusses, new FileStatusModificationComparator());
            MinMaxJobFileTracker minMaxJobFileTracker = new MinMaxJobFileTracker();
            int batchCount = BatchUtil.getBatchCount(jobFileStatusses.length, batchSize);
            for (int b = 0; b < batchCount; b++) {
                LOG.debug("=============   Pre-Process Start ===========");
                ProcessRecord processRecord = processBatch(jobFileStatusses, b, batchSize, cluster);
                minMaxJobFileTracker.track(processRecord.getMinJobId());
                minMaxJobFileTracker.track(processRecord.getMaxJobId());
                LOG.debug("=============   Pre-Process End ===========");

                LOG.debug("=============   Load Raw history Start  ==============");
                Iterator it = jobstatusmap.entrySet().iterator();
                List<Put> puts = new LinkedList<Put>();
                while (it.hasNext()) {
                    Map.Entry<JobFile, FileStatus> pairs = (Map.Entry) it.next();
                    loadRawHistory(pairs.getKey(), pairs.getValue(), puts);
                }
                rawTable.put(puts);
                rawTable.flushCommits();
                processRecordService.setProcessState(processRecord, ProcessState.LOADED);
                LOG.debug("=============   Load Raw history End  ==============");
                jobstatusmap.clear();
            }
            loadJobTaskDetail(cluster, minMaxJobFileTracker.getMinJobId(), minMaxJobFileTracker.getMaxJobId());
            Thread.sleep(INTERVAL);
      }

        private FileStatus[] findProcessFiles() throws IOException {

            long processingStartMillis = System.currentTimeMillis();

            // Figure out where we last left off (if anywhere at all)
            ProcessRecord lastProcessRecord;

            lastProcessRecord = processRecordService
                    .getLastSuccessfulProcessRecord(cluster);

            long minModificationTimeMillis = 0;
            if (lastProcessRecord != null) {
                // Start of this time period is the end of the last period.
                minModificationTimeMillis = lastProcessRecord
                        .getMaxModificationTimeMillis();
            }

            // Do a sanity check. The end time of the last scan better not be later
            // than when we started processing.
            if (minModificationTimeMillis > processingStartMillis) {
                throw new RuntimeException(
                        "The last processing record has maxModificationMillis later than now: "
                                + lastProcessRecord);
            }

            // Accept only jobFiles and only those that fall in the desired range of
            // modification time.
            JobFileModifiedRangePathFilter jobFileModifiedRangePathFilter = new JobFileModifiedRangePathFilter(
                    hbaseConf, minModificationTimeMillis);

            String timestamp = Constants.TIMESTAMP_FORMAT.format(new Date(
                    minModificationTimeMillis));

            ContentSummary contentSummary = hdfs.getContentSummary(inputPath);
            LOG.info("Listing / filtering ("
                    + contentSummary.getFileCount() + ") files in: " + inputPath
                    + " that are modified since " + timestamp);

            // get the files in the done folder,
            // need to traverse dirs under done recursively for versions
            // that include MAPREDUCE-323: on/after hadoop 0.20.203.0
            // on/after cdh3u5
            return FileLister.getListFilesToProcess(maxFileSize, true,
                   hdfs, inputPath, jobFileModifiedRangePathFilter);
        }

        private void loadJobTaskDetail(String cluster, String minJobId, String maxJobId) throws IOException, RowKeyParseException {
            ResultScanner scanner = rawService.getHistoryRawTableScans(cluster, minJobId,maxJobId);

            List<Put> jobputs = new LinkedList<Put>();
            List<Put> taskputs = new LinkedList<Put>();
            for (Result result : scanner) {
                QualifiedJobId qualifiedJobId = null;
                jobputs.clear();
                taskputs.clear();
                Configuration jobConf;
                byte[] jobhistoryraw;
                byte[] historyFileContents;
                try {
                    qualifiedJobId = rawService.getQualifiedJobIdFromResult(result);
                    jobConf = rawService.createConfigurationFromResult(result);
                    jobhistoryraw = rawService.getJobHistoryRawFromResult(result);
                    // 1. query completed job with user name and job FINISHED time
                    // 2. query completed job with user name, SUBMIT time, and job name
                    long submitTimeMillis = JobHistoryFileParserBase.getSubmitTimeMillisFromJobHistory(jobhistoryraw);
                    if (submitTimeMillis == 0L) {
                        LOG.info("NOTE: Since submitTimeMillis from job history is 0, now attempting to "
                                + "approximate job start time based on last modification time from the raw table");
                        submitTimeMillis = rawService.getApproxSubmitTime(result);
                    }

                    long finishTimeMillis = ExtendJobHistoryFileParserHadoop2.getFinishTimeMillisFromJobHistory(jobhistoryraw);
                    if (finishTimeMillis == 0L) {
                        finishTimeMillis = rawService.getApproxFinishTime(result);
                        LOG.info("NOTE: Since finishTimeMillis from job history is 0, now approximate job finish time to "
                                +finishTimeMillis+ " based on last modification time from the raw table");

                    }
                    Put finishTimePut = rawService.getJobFinishTimePut(result.getRow(), finishTimeMillis);
                    rawTable.put(finishTimePut);

                    // use finishTime to create JobDesc insteads of startTime
                    JobDesc jobDesc_w_finishT = JobDescFactory.createJobDesc(qualifiedJobId, finishTimeMillis, jobConf);
                    JobKey jobKey_w_finishT = new JobKey(jobDesc_w_finishT);
                    LOG.info("JobDesc with finish time (" + keyCount + "): " + jobDesc_w_finishT + " finishTimeMillis: " + finishTimeMillis);

                    //user job sorted by submit time
                    JobDesc jobDesc_w_submitT = JobDescFactory.createJobDesc(qualifiedJobId, submitTimeMillis, jobConf);
                    JobKey jobKey_w_submitT = new JobKey(jobDesc_w_submitT);
                    LOG.info("JobDesc with submit time (" + keyCount + "): " + jobDesc_w_submitT + " submitTimeMillis: " + submitTimeMillis);

                    List<Put> puts = ExtendJobHistoryService.getHbasePuts(jobDesc_w_submitT,jobDesc_w_finishT , jobConf);
                    LOG.info("Writing " + puts.size() + " JobConf puts to "  + Constants.HISTORY_TABLE);
                    jobputs.addAll(puts);

                    LOG.info("Writing secondary indexes");
                    jobHistoryByIdService.writeIndexes(jobKey_w_submitT);
                    writeJobUser(jobKey_w_submitT);
                    //TODO:

                    KeyValue keyValue = result.getColumnLatest(Constants.RAW_FAM_BYTES,  Constants.JOBHISTORY_COL_BYTES);
                    if (keyValue == null) {
                        throw new MissingColumnInResultException(Constants.RAW_FAM_BYTES,
                                Constants.JOBHISTORY_COL_BYTES);
                    } else {
                        historyFileContents = keyValue.getValue();
                    }

                    ExtendJobHistoryFileParserHadoop2 historyFileParser = new ExtendJobHistoryFileParserHadoop2(jobConf);
                    historyFileParser.parse(historyFileContents, jobKey_w_submitT, jobKey_w_finishT);

                    puts = historyFileParser.getJobPuts();
                    if (puts == null) {
                        throw new ProcessingException(" Unable to get job puts for this record!" + jobKey_w_finishT);
                    } else{
                        LOG.info("Writing " + puts.size() + " Job puts to "  + Constants.HISTORY_TABLE);
                        jobputs.addAll(puts);
                    }

                    puts = historyFileParser.getTaskPuts();
                    if (puts == null) {
                        throw new ProcessingException(" Unable to get task puts for this record!" + jobKey_w_finishT);
                    }else {
                        LOG.info("Writing " + puts.size() + " task puts to " + Constants.HISTORY_TASK_TABLE);
                        taskputs.addAll(puts);
                    }

                    // Usage of NCHC SU is most equivalent to MegaByteMill
                    // I DO NOT change the variable from MegaByteMillis to SU
                    // just modify the web UI display name
                    Long su = historyFileParser.getSU(Long.parseLong(this.prop.getProperty("CPC", "1")));
                    LOG.debug(jobKey_w_finishT.getJobId() +" SU = " + su);

                    List<Put> mbPut = PutUtil.getMegaByteMillisPut(su, jobKey_w_submitT,jobKey_w_finishT);
                    LOG.info("Writing SU puts to " + Constants.HISTORY_TABLE);
                    jobputs.addAll(mbPut);


                    // post processing steps to get cost of the job
                    Double jobCost = PutUtil.getJobCost(su, Double.parseDouble(this.prop.getProperty("SU", "1.7")));
                    LOG.debug(jobKey_w_finishT.getJobId() +" cost = " + jobCost);
                    if (jobCost == null) {
                        throw new ProcessingException(" Unable to get job cost calculation for this record!"
                                + jobKey_w_finishT);
                    }
                    List<Put> jobCostPut = PutUtil.getJobCostPut(jobCost, jobKey_w_submitT, jobKey_w_finishT);
                    LOG.info("Writing jobCost puts to " + Constants.HISTORY_TABLE);
                    jobputs.addAll(jobCostPut);


                    jobTable.put(jobputs);
                    taskTable.put(taskputs);
                }catch (RowKeyParseException rkpe) {
                    LOG.error("Failed to process record "
                        + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), rkpe);
                } catch (MissingColumnInResultException mcire) {
                    LOG.error("Failed to process record "
                        + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), mcire);
                } catch (ProcessingException pe) {
                    LOG.error("Failed to process record "
                        + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), pe);
                } catch (IllegalArgumentException iae) {
                    LOG.error("Failed to process record "
                        + (qualifiedJobId != null ? qualifiedJobId.toString() : ""),iae);
                }
            }
        }

        private void writeJobUser(JobKey jobKey) throws IOException {
            Put p = new Put(ExtendConstants.SEP5_BYTES);
            p.add(Bytes.toBytes("u"),Bytes.toBytes(jobKey.getUserName()),null);
            runningTable.put(p);
            LOG.info(String.format("Write user name [%s] into table [%s]",
                    jobKey.getUserName(),ExtendConstants.RUNNING_JOB_TABLE));
        }

        private byte[] getRowKeyBytes(JobFile jobFile) {
            String cluster = hbaseConf.get(Constants.CLUSTER_JOB_CONF_KEY);
            return rawService.getRowKey(cluster, jobFile.getJobid());
        }

        private void loadRawHistory(JobFile jobFile, FileStatus fileStatus, List<Put> puts) throws IOException {

            boolean exists = hdfs.exists(fileStatus.getPath());
            if (exists) {
                // Determine if we need to process this file.
                if (jobFile.isJobConfFile()) {
                    keyCount++;
                    byte[] rowKey = getRowKeyBytes(jobFile);
                    PutUtil.addFileNamePut(puts, rowKey, Constants.JOBCONF_FILENAME_COL_BYTES,
                            jobFile.getFilename());
                    PutUtil.addRawPut(puts, rowKey, Constants.JOBCONF_COL_BYTES,
                            Constants.JOBCONF_LAST_MODIFIED_COL_BYTES, fileStatus,hdfs);
                    LOG.info("Loaded conf file (" + keyCount + ") size: "
                            + fileStatus.getLen() + " = " + jobFile.getFilename());
                } else if (jobFile.isJobHistoryFile()) {
                    keyCount++;
                    byte[] rowKey = getRowKeyBytes(jobFile);
                    // Add filename to be used to re-create JobHistory URL later
                    PutUtil.addFileNamePut(puts, rowKey, Constants.JOBHISTORY_FILENAME_COL_BYTES,
                            jobFile.getFilename());
                    PutUtil.addRawPut(puts, rowKey, Constants.JOBHISTORY_COL_BYTES,
                            Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES, fileStatus,hdfs);
                    LOG.info("Loaded history file (" + keyCount + ") size: "
                            + fileStatus.getLen() + " = " + jobFile.getFilename());
                } else {
                    LOG.warn("Skipping Key: " + jobFile.getFilename());
                }
            } else {
                LOG.warn("Unable to find file: " + fileStatus.getPath());
            }
        }

        /**
         * @param jobFileStatusses
         *          statusses sorted by modification time.
         * @param batch
         *          which batch needs to be processed (used to calculate offset in
         *          jobFileStatusses.
         * @param batchSize
         *          process up to length items (or less as to not exceed the length of
         *          jobFileStatusses
         * @throws java.io.IOException
         *           when the index file cannot be written or moved, or when the HBase
         *           records cannot be created.
         */
        private ProcessRecord processBatch(FileStatus jobFileStatusses[],int batch, int batchSize, String cluster) throws IOException {

            int startIndex = batch * batchSize;
            int endIndexExclusive = Math.min((startIndex + batchSize), jobFileStatusses.length);
            MinMaxJobFileTracker minMaxJobFileTracker = new MinMaxJobFileTracker();

            for (int i =startIndex;i<endIndexExclusive;i++) {
                jobstatusmap.put(minMaxJobFileTracker.track(jobFileStatusses[i]), jobFileStatusses[i]);
            }
            ProcessRecord processRecord = new ProcessRecord(cluster,  ProcessState.PREPROCESSED,
                    minMaxJobFileTracker.getMinModificationTimeMillis(),
                    minMaxJobFileTracker.getMaxModificationTimeMillis(), endIndexExclusive - startIndex,
                    "N/A", minMaxJobFileTracker.getMinJobId(),
                    minMaxJobFileTracker.getMaxJobId());

            LOG.info("Creating processRecord: " + processRecord);

            processRecordService.writeJobRecord(processRecord);

            return processRecord;
        }


        public void stopThread(){
            try {

                rawService.close();
                LOG.info("Stop raw service");
                jobHistoryByIdService.close();
                LOG.info("Stop jobHistoryByIdService");
                processRecordService.close();
                LOG.info("Stop processRecordService");
                if (rawTable != null){
                    rawTable.close();
                    LOG.info("STOP raw Htable");
                }
                if (jobTable != null) {
                    jobTable.close();
                    LOG.info("STOP jobTable Htable");
                }
                if (taskTable != null) {
                    taskTable.close();
                    LOG.info("STOP taskTable Htable");
                }
            }catch (IOException e){
                StringWriter errors = new StringWriter();
                e.printStackTrace(new PrintWriter(errors));
                LOG.error(errors.toString());
            }
            isRunning = false;
        }
    }
