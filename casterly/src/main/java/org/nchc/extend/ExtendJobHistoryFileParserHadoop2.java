package org.nchc.extend;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import com.twitter.hraven.*;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.TaskKeyConverter;
import com.twitter.hraven.etl.JobHistoryFileParserBase;
import com.twitter.hraven.etl.JobHistoryFileParserFactory;
import com.twitter.hraven.util.ByteArrayWrapper;
import com.twitter.hraven.util.ByteUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobHistoryCopy.RecordTypes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Maps;

/**
 * Created by 1403035 on 2014/11/28.
 */
public class ExtendJobHistoryFileParserHadoop2 extends JobHistoryFileParserBase {
    private JobKey jobKey;
    /** Job ID, minus the leading "job_" */
    private String jobNumber = "";
    private byte[] jobKeyBytes;
    private byte[] jobKeyByTS;
    private List<Put> jobPuts = new LinkedList<Put>();
    private List<Put> taskPuts = new LinkedList<Put>();
    boolean uberized = false;

    /**
     * Stores the terminal status of the job
     *
     * Since this history file is placed hdfs at mapreduce.jobhistory.done-dir
     * only upon job termination, we ensure that we store the status seen
     * only in one of the terminal state events in the file like
     * JobFinished(JOB_FINISHED) or JobUnsuccessfulCompletion(JOB_FAILED, JOB_KILLED)
     *
     * Ideally, each terminal state event like JOB_FINISHED, JOB_FAILED, JOB_KILLED
     * should contain the jobStatus field and we would'nt need this extra processing
     * But presently, in history files, only JOB_FAILED, JOB_KILLED events
     * contain the jobStatus field where as JOB_FINISHED event does not,
     * hence this extra processing
     */
    private String jobStatus = "";
    /** hadoop2 JobState enum:
     * NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED, ERROR
     */
    public static final String JOB_STATUS_SUCCEEDED = "SUCCEEDED";

    /** explicitly initializing map millis and
     * reduce millis in case it's not found
     */
    private long mapSlotMillis = 0L;
    private long reduceSlotMillis = 0L;

    private Map<String, DurationPair> mapAttemptMap = new HashMap<String, DurationPair>();
    private Map<String, DurationPair> reduceAttemptMap = new HashMap<String, DurationPair>();

    private long startTime = ExtendConstants.NOTFOUND_VALUE;
    private long endTime = ExtendConstants.NOTFOUND_VALUE;
    private static final String LAUNCH_TIME_KEY_STR = JobHistoryKeys.LAUNCH_TIME.toString();
    private static final String FINISH_TIME_KEY_STR = JobHistoryKeys.FINISH_TIME.toString();

    private ExtendJobKeyConverter jobKeyConv = new ExtendJobKeyConverter();
    private TaskKeyConverter taskKeyConv = new TaskKeyConverter();

    private static final String AM_ATTEMPT_PREFIX = "AM_";
    private static final String TASK_PREFIX = "task_";
    private static final String TASK_ATTEMPT_PREFIX = "attempt_";

    private static final Log LOG = LogFactory.getLog(ExtendJobHistoryFileParserHadoop2.class);

    private Schema schema;
    private Decoder decoder;
    private DatumReader<GenericRecord> reader;

    private static final String TYPE = "type";
    private static final String EVENT = "event";
    private static final String NAME = "name";
    private static final String FIELDS = "fields";
    private static final String COUNTS = "counts";
    private static final String GROUPS = "groups";
    private static final String VALUE = "value";
    private static final String TASKID = "taskid";
    private static final String APPLICATION_ATTEMPTID = "applicationAttemptId";
    private static final String ATTEMPTID = "attemptId";

    private static final String TYPE_INT = "int";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_LONG = "long";
    private static final String TYPE_STRING = "String";
    /** only acls in the job history file seem to be of this type: map of strings */
    private static final String TYPE_MAP_STRINGS = "{\"type\":\"map\",\"values\":\"string\"}";
    /**
     * vMemKbytes, clockSplit, physMemKbytes, cpuUsages are arrays of ints See MAPREDUCE-5432
     */
    private static final String TYPE_ARRAY_INTS = "{\"type\":\"array\",\"items\":\"int\"}";
    /** this is part of {@link org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent.java} */
    private static final String NULL_STRING = "[\"null\",\"string\"]";

    public static enum Hadoop2RecordType {
        /**
         * populating this map since the symbols and key to get the types of fields the symbol denotes
         * the record in the file (like JOB_SUBMITTED) and it's value in the map (like JobSubmitted)
         * helps us get the types of fields that that record contains (this type information is present
         * in the schema)
         */
        JobFinished("JOB_FINISHED"),
        JobInfoChange("JOB_INFO_CHANGED"),
        JobInited("JOB_INITED"),
        AMStarted("AM_STARTED"),
        JobPriorityChange("JOB_PRIORITY_CHANGED"),
        JobStatusChanged("JOB_STATUS_CHANGED"),
        JobQueueChange("JOB_QUEUE_CHANGED"),
        JobSubmitted("JOB_SUBMITTED"),
        JobUnsuccessfulCompletion("JOB_KILLED","JOB_FAILED"),

        //Map Attempt type
        MapAttemptStarted("MAP_ATTEMPT_STARTED"),
        MapAttemptFinished("MAP_ATTEMPT_FINISHED"),
        MapAttemptUnsuccessfulCompletion("MAP_ATTEMPT_KILLED","MAP_ATTEMPT_FAILED"),
        //Reduce Attempt type
        ReduceAttemptFinished("REDUCE_ATTEMPT_FINISHED"),
        ReduceAttemptStarted("REDUCE_ATTEMPT_STARTED"),
        ReduceAttemptUnsuccessfulCompletion("REDUCE_ATTEMPT_KILLED",  "REDUCE_ATTEMPT_FAILED"),
        //SETUP & CLEANUP Attempt type
        TaskAttemptFinished("CLEANUP_ATTEMPT_FINISHED","SETUP_ATTEMPT_FINISHED"),
        TaskAttemptStarted("CLEANUP_ATTEMPT_STARTED","SETUP_ATTEMPT_STARTED"),
        TaskAttemptUnsuccessfulCompletion(
                "CLEANUP_ATTEMPT_KILLED",
                "CLEANUP_ATTEMPT_FAILED",
                "SETUP_ATTEMPT_KILLED",
                "SETUP_ATTEMPT_FAILED"
        ),
        //Task
        TaskFailed("TASK_FAILED"),
        TaskFinished("TASK_FINISHED"),
        TaskStarted("TASK_STARTED"),
        TaskUpdated("TASK_UPDATED");

        private final String[] recordNames;

        private Hadoop2RecordType(String... recordNames) {
            if (recordNames != null) {
                this.recordNames = recordNames;
            } else {
                this.recordNames = new String[0];
            }
        }

        public String[] getRecordNames() {
            return recordNames;
        }
    }

    public static enum CounterTypes {
        counters, mapCounters, reduceCounters, totalCounters
    }
    private static Map<String,Hadoop2RecordType> EVENT_RECORD_NAMES = Maps.newHashMap();
    private static final Set<String> COUNTER_NAMES = new HashSet<String>();
    private Map<Hadoop2RecordType, Map<String, String>> fieldTypes =
            new HashMap<Hadoop2RecordType, Map<String, String>>();

    /**
     * populates the COUNTER_NAMES hash set and EVENT_RECORD_NAMES hash map
     */
    static {
        /**
         * populate the hash set for counter names
         */
        for (CounterTypes ct : CounterTypes.values()) {
            COUNTER_NAMES.add(ct.toString());
        }

        /**
         * populate the hash map of EVENT_RECORD_NAMES
         */
        for (Hadoop2RecordType t : Hadoop2RecordType.values()) {
            for (String name : t.getRecordNames()) {
                EVENT_RECORD_NAMES.put(name, t);
            }
        }

    }

    public ExtendJobHistoryFileParserHadoop2(Configuration conf) {
        super(conf);
    }

    public void parse(byte[] historyFileContents, JobKey jobKey_w_submitT, JobKey jobKey_w_finishT)
            throws ProcessingException {
        this.jobKeyByTS = jobKeyConv.toBytesSortByTS(jobKey_w_finishT);
        parse(historyFileContents,jobKey_w_submitT);
    }




    /**
     * {@inheritDoc}
     */
    @Override
    public void parse(byte[] historyFileContents, JobKey jobKey)
            throws ProcessingException {

        this.jobKey = jobKey;
        this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
        setJobId(jobKey.getJobId().getJobIdString());
        mapAttemptMap.clear();
        reduceAttemptMap.clear();

        try {
            FSDataInputStream in =
                    new FSDataInputStream(new ByteArrayWrapper(historyFileContents));

            /** first line is the version, ignore it */
            String versionIgnore = in.readLine();

            /** second line in file is the schema */
            this.schema = schema.parse(in.readLine());

            /** now figure out the schema */
            understandSchema(schema.toString());

            /** now read the rest of the file */
            this.reader = new GenericDatumReader<GenericRecord>(schema);
            this.decoder = DecoderFactory.get().jsonDecoder(schema, in);

            GenericRecord record = null;
            Hadoop2RecordType recType = null;
            try {
                while ((record = reader.read(null, decoder)) != null) {
                    if (record.get(TYPE) != null) {
                        recType = EVENT_RECORD_NAMES.get(record.get(TYPE).toString());
                    } else {
                        throw new ProcessingException("expected one of "
                                + Arrays.asList(Hadoop2RecordType.values())
                                + " \n but not found, cannot process this record! " + jobKey);
                    }
                    if (recType == null) {
                        throw new ProcessingException("new record type has surfaced: "
                                + record.get(TYPE).toString() + " cannot process this record! " + jobKey);
                    }
                    // GenericRecord's get returns an Object
                    Object eDetails = record.get(EVENT);

                    // confirm that we got an "event" object
                    if (eDetails != null) {
                        JSONObject eventDetails = new JSONObject(eDetails.toString());
                        processRecords(recType, eventDetails);
                    } else {
                        throw new ProcessingException("expected event details but not found "
                                + record.get(TYPE).toString() + " cannot process this record! " + jobKey);
                    }
                }
            } catch (EOFException eof) {
                // not an error, simply end of file
                LOG.info("Done parsing file, reached eof for " + jobKey);
            }
        } catch (IOException ioe) {
            throw new ProcessingException(" Unable to parse history file in function parse, "
                    + "cannot process this record!" + jobKey + " error: ", ioe);
        } catch (JSONException jse) {
            throw new ProcessingException(" Unable to parse history file in function parse, "
                    + "cannot process this record! " + jobKey + " error: ", jse);
        } catch (IllegalArgumentException iae) {
            throw new ProcessingException(" Unable to parse history file in function parse, "
                    + "cannot process this record! " + jobKey + " error: ", iae);
        }

    /*
     * set the job status for this job once the entire file is parsed
     * this has to be done separately
     * since JOB_FINISHED event is missing the field jobStatus,
     * where as JOB_KILLED and JOB_FAILED
     * events are not so we need to look through the whole file to confirm
     * the job status and then generate the put
     */
//  Put jobStatusPut = getJobStatusPut();
//  this.jobPuts.add(jobStatusPut);
        List<Put> ps = getJobStatusPuts();
        this.jobPuts.addAll(ps);

        // set the hadoop version for this record
        Put versionPut = getHadoopVersionPut(JobHistoryFileParserFactory.getHistoryFileVersion2(), this.jobKeyBytes);
        Put versionPut1 = getHadoopVersionPut(JobHistoryFileParserFactory.getHistoryFileVersion2(), this.jobKeyByTS);
        this.jobPuts.add(versionPut);
        this.jobPuts.add(versionPut1);

        LOG.info("For " + this.jobKey + " #jobPuts " + jobPuts.size() + " #taskPuts: "
                + taskPuts.size());
    }

    /**
     * generates a put for job status
     * @return Put that contains Job Status
     */
    private Put getJobStatusPut() {
        Put pStatus = new Put(jobKeyBytes);
        byte[] valueBytes = Bytes.toBytes(this.jobStatus);
        byte[] qualifier = Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase());
        pStatus.add(ExtendConstants.INFO_FAM_BYTES, qualifier, valueBytes);
        return pStatus;
    }

    private List<Put> getJobStatusPuts() {
        List<Put> lp = new LinkedList<Put>();
        Put pStatus;
        pStatus= new Put(jobKeyBytes);
        byte[] valueBytes = Bytes.toBytes(this.jobStatus);
        byte[] qualifier = Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase());
        pStatus.add(ExtendConstants.INFO_FAM_BYTES, qualifier, valueBytes);
        lp.add(pStatus);
        pStatus= new Put(jobKeyByTS);
        valueBytes = Bytes.toBytes(this.jobStatus);
        qualifier = Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase());
        pStatus.add(ExtendConstants.INFO_FAM_BYTES, qualifier, valueBytes);
        lp.add(pStatus);
        return lp;
    }

    /**
     * understand the schema so that we can parse the rest of the file
     * @throws JSONException
     */
    private void understandSchema(String schema) throws JSONException {

        JSONObject j1 = new JSONObject(schema);
        JSONArray fields = j1.getJSONArray(FIELDS);

        String fieldName;
        String fieldTypeValue;
        Object recName;

        for (int k = 0; k < fields.length(); k++) {
            if (fields.get(k) == null) {
                continue;
            }
            JSONObject allEvents = new JSONObject(fields.get(k).toString());
            Object name = allEvents.get(NAME);
            if (name != null) {
                if (name.toString().equalsIgnoreCase(EVENT)) {
                    JSONArray allTypeDetails = allEvents.getJSONArray(TYPE);
                    for (int i = 0; i < allTypeDetails.length(); i++) {
                        JSONObject actual = (JSONObject) allTypeDetails.get(i);
                        JSONArray types = actual.getJSONArray(FIELDS);
                        Map<String, String> typeDetails = new HashMap<String, String>();
                        for (int j = 0; j < types.length(); j++) {
                            if (types.getJSONObject(j) == null ) {
                                continue;
                            }
                            fieldName = types.getJSONObject(j).getString(NAME);
                            fieldTypeValue = types.getJSONObject(j).getString(TYPE);
                            if ((fieldName != null) && (fieldTypeValue != null)) {
                                typeDetails.put(fieldName, fieldTypeValue);
                            }
                        }

                        recName = actual.get(NAME);
                        if (recName != null) {
              /* the next statement may throw an IllegalArgumentException if
               * it finds a new string that's not part of the Hadoop2RecordType enum
               * that way we know what types of events we are parsing
               */
                            fieldTypes.put(Hadoop2RecordType.valueOf(recName.toString()), typeDetails);
                        }
                    }
                }
            }
        }
    }

    /**
     * process the counter details example line in .jhist file for counters: { "name":"MAP_COUNTERS",
     * "groups":[ { "name":"org.apache.hadoop.mapreduce.FileSystemCounter",
     * "displayName":"File System Counters", "counts":[ { "name":"HDFS_BYTES_READ",
     * "displayName":"HDFS: Number of bytes read", "value":480 }, { "name":"HDFS_BYTES_WRITTEN",
     * "displayName":"HDFS: Number of bytes written", "value":0 } ] }, {
     * "name":"org.apache.hadoop.mapreduce.TaskCounter", "displayName":"Map-Reduce Framework",
     * "counts":[ { "name":"MAP_INPUT_RECORDS", "displayName":"Map input records", "value":10 }, {
     * "name":"MAP_OUTPUT_RECORDS", "displayName":"Map output records", "value":10 } ] } ] }
     */
    private void processCounters(Put p, JSONObject eventDetails, String key) {

        try {
            JSONObject jsonCounters = eventDetails.getJSONObject(key);
            String counterMetaGroupName = jsonCounters.getString(NAME);
            JSONArray groups = jsonCounters.getJSONArray(GROUPS);
            for (int i = 0; i < groups.length(); i++) {
                JSONObject aCounter = groups.getJSONObject(i);
                JSONArray counts = aCounter.getJSONArray(COUNTS);
                for (int j = 0; j < counts.length(); j++) {
                    JSONObject countDetails = counts.getJSONObject(j);
                    populatePut(p, ExtendConstants.INFO_FAM_BYTES, counterMetaGroupName, aCounter.get(NAME)
                            .toString(), countDetails.get(NAME).toString(), countDetails.getLong(VALUE));
                }
            }
        } catch (JSONException e) {
            throw new ProcessingException(" Caught json exception while processing counters ", e);
        }

    }

    /**
     * process the event details as per their data type from schema definition
     * @throws JSONException
     */
    private void
    processAllTypes(Put p, Hadoop2RecordType recType, JSONObject eventDetails, String key)
            throws JSONException {

        if (COUNTER_NAMES.contains(key)) {
            processCounters(p, eventDetails, key);
        } else {
            String type;
            Map<String, String> tm = fieldTypes.get(recType);
            if(fieldTypes.get(recType) == null)
                type = TYPE_STRING;
            else
                type = tm.get(key);

            if (type.equalsIgnoreCase(TYPE_STRING)) {
                // look for job status
                if (JobHistoryKeys.JOB_STATUS.toString().equals(
                        JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key))) {
                    // store it only if it's one of the terminal state events
                    if ((recType.equals(Hadoop2RecordType.JobFinished))
                            || (recType.equals(Hadoop2RecordType.JobUnsuccessfulCompletion))) {
                        this.jobStatus = eventDetails.getString(key);
                    }
                } else {
                    String value = eventDetails.getString(key);
                    populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, value);
                }
            } else if (type.equalsIgnoreCase(TYPE_LONG)) {
                long value = eventDetails.getLong(key);
                populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, value);
                // populate start time of the job for megabytemillis calculations
                if ((recType.equals(Hadoop2RecordType.JobInited)) &&
                        LAUNCH_TIME_KEY_STR.equals(JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key))) {
                    this.startTime = value;
                }
                // populate end time of the job for megabytemillis calculations
                if ((recType.equals(Hadoop2RecordType.JobFinished))
                        || (recType.equals(Hadoop2RecordType.JobUnsuccessfulCompletion))) {
                    if (FINISH_TIME_KEY_STR.equals(JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key))) {
                        this.endTime = value;
                    }
                }
            } else if (type.equalsIgnoreCase(TYPE_INT)) {
                int value = eventDetails.getInt(key);
                populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, value);
            } else if (type.equalsIgnoreCase(TYPE_BOOLEAN)) {
                boolean value = eventDetails.getBoolean(key);
                populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, Boolean.toString(value));
            } else if (type.equalsIgnoreCase(TYPE_ARRAY_INTS)) {
                String value = eventDetails.getString(key);
                populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, value);
            } else if (type.equalsIgnoreCase(NULL_STRING)) {
                // usually seen in FAILED tasks
                String value = eventDetails.getString(key);
                populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, value);
            } else if (type.equalsIgnoreCase(TYPE_MAP_STRINGS)) {
                JSONObject ms = new JSONObject(eventDetails.get(key).toString());
                populatePut(p, ExtendConstants.INFO_FAM_BYTES, key, ms.toString());
            } else {
                throw new ProcessingException("Encountered a new type " + type
                        + " unable to complete processing " + this.jobKey);
            }
        }
    }

    /**
     * iterate over the event details and prepare puts
     * @throws JSONException
     */
    private void iterateAndPreparePuts(JSONObject eventDetails, Put p, Hadoop2RecordType recType)
            throws JSONException {
        Iterator<?> keys = eventDetails.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            processAllTypes(p, recType, eventDetails, key);
        }
    }

    /**
     * process individual records
     * @throws JSONException
     */
    private void processRecords(Hadoop2RecordType recType, JSONObject eventDetails)
            throws JSONException {
        switch (recType) {
            case JobFinished:
                // this setting is needed since the job history file is missing
                // the jobStatus field in the JOB_FINISHED event
                this.jobStatus = JOB_STATUS_SUCCEEDED;
            case JobInfoChange:
            case JobInited:
            case JobPriorityChange:
            case JobStatusChanged:
            case JobQueueChange:
            case JobSubmitted:
            case JobUnsuccessfulCompletion:
                // For scan Job sort by Name
                Put pJob = new Put(this.jobKeyBytes);
                iterateAndPreparePuts(eventDetails, pJob, recType);
                this.jobPuts.add(pJob);
                LOG.debug("add to Put list | recType = [ "+recType+" ] | RK = [ "+ Bytes.toStringBinary(jobKeyBytes)+" ]");
                // for scan Job sort by TS
                Put pJobT = new Put(this.jobKeyByTS);
                iterateAndPreparePuts(eventDetails, pJobT, recType);
                this.jobPuts.add(pJobT);
                LOG.debug("add to Put list | recType = [ "+recType+" ] | RK = [ "+ Bytes.toStringBinary(jobKeyBytes)+" ]");
                break;

            case AMStarted:
                byte[] amAttemptIdKeyBytes =
                        getAMKey(AM_ATTEMPT_PREFIX, eventDetails.getString(APPLICATION_ATTEMPTID));
                // generate a new put per AM Attempt
                Put pAM = new Put(amAttemptIdKeyBytes);
                pAM.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.RECORD_TYPE_COL_BYTES,
                        Bytes.toBytes(RecordTypes.Task.toString()));
                iterateAndPreparePuts(eventDetails, pAM, recType);
                taskPuts.add(pAM);
                LOG.debug("add to task list | recType = [ "+recType+" ] | RK = [ "+ Bytes.toStringBinary(amAttemptIdKeyBytes)+" ]");
                break;

            case MapAttemptStarted:
                keepAttemptStartTime(recType, eventDetails,mapAttemptMap);
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.MapAttempt);
                break;
            case MapAttemptFinished:
                keepAttemptEndTime(recType, eventDetails,mapAttemptMap);
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.MapAttempt);
                break;
            case MapAttemptUnsuccessfulCompletion:
                keepAttemptFailTime(recType, eventDetails, mapAttemptMap);
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.MapAttempt);
                break;
            case ReduceAttemptStarted:
                keepAttemptStartTime(recType, eventDetails,reduceAttemptMap);
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.ReduceAttempt);
                break;
            case ReduceAttemptFinished:
                keepAttemptEndTime(recType, eventDetails,reduceAttemptMap);
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.ReduceAttempt);
                break;
            case ReduceAttemptUnsuccessfulCompletion:
                keepAttemptFailTime(recType, eventDetails, reduceAttemptMap);
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.ReduceAttempt);
                break;


            case TaskAttemptFinished:
            case TaskAttemptStarted:
            case TaskAttemptUnsuccessfulCompletion:
                addToTaskAttemptPutList(recType, eventDetails, RecordTypes.Task);
                break;

            case TaskFailed:
            case TaskStarted:
            case TaskUpdated:
            case TaskFinished:
                byte[] taskIdKeyBytes =
                        getNewTaskKey(TASK_PREFIX, this.jobNumber, eventDetails.getString(TASKID));
                Put pTask = new Put(taskIdKeyBytes);
                pTask.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.RECORD_TYPE_COL_BYTES,
                        Bytes.toBytes(RecordTypes.Task.toString()));
                iterateAndPreparePuts(eventDetails, pTask, recType);
                taskPuts.add(pTask);
                LOG.debug("add to task list | recType = [ "+recType+" ] | RK = [ " + Bytes.toStringBinary(taskIdKeyBytes)+" ]");
                break;
            default:
                LOG.error("Check if recType was modified and has new members?");
                throw new ProcessingException("Check if recType was modified and has new members? " + recType);
        }
    }

    private void keepAttemptStartTime(Hadoop2RecordType recType,
                          JSONObject eventDetails,
                          Map<String, DurationPair> map)throws JSONException{
        String attId = eventDetails.getString(ATTEMPTID);
        long starttime = eventDetails.getLong("startTime");
        if (map.containsKey(attId)) {
            map.get(attId).setStart(starttime);
            LOG.debug( "| recType = [ "+recType+" ] | "+ "UPDATE ts "+ map.get(attId) + " for "+attId);
        } else {
            map.put(attId,new DurationPair(starttime,0,0));
            LOG.debug("| recType = [ "+recType+" ] | "+ "CREATE ts " + map.get(attId) +"for " + attId);
        }
    }

    private void keepAttemptEndTime(Hadoop2RecordType recType,
                                          JSONObject eventDetails,
                                          Map<String, DurationPair> map)throws JSONException{
        String attId = eventDetails.getString(ATTEMPTID);
        long endtime = eventDetails.getLong("finishTime");
        if (map.containsKey(attId)) {
            map.get(attId).setEnd(endtime);
            LOG.debug("| recType = [ "+recType+" ] | "+ "UPDATE ts "+ map.get(attId) + " for "+attId);
        } else {
            map.put(attId,new DurationPair(0,endtime,0));
            LOG.debug("| recType = [ "+recType+" ] | "+ "CREATE ts " + map.get(attId) +"for " + attId);
        }
    }


    private void keepAttemptFailTime(Hadoop2RecordType recType,
                                    JSONObject eventDetails,
                                    Map<String, DurationPair> map)throws JSONException{
        String attId = eventDetails.getString(ATTEMPTID);
        long endtime = eventDetails.getLong("finishTime");
        if (map.containsKey(attId)) {
            map.get(attId).setFail(endtime);
            LOG.debug("| recType = [ "+recType+" ] | "+ "UPDATE ts "+ map.get(attId) + " for "+attId);
        } else {
            map.put(attId,new DurationPair(0,0,endtime));
            LOG.debug("| recType = [ "+recType+" ] | "+  "CREATE ts " + map.get(attId) +"for " + attId);
        }
    }

    private void addToTaskAttemptPutList(Hadoop2RecordType recType,
                                         JSONObject eventDetails,
                                         RecordTypes type)throws JSONException{
        String  attemptType;
        switch(type){
            case MapAttempt:
                attemptType = ExtendConstants.MAP_ATTEMPT_TYPE;
                break;
            case ReduceAttempt:
                attemptType = ExtendConstants.REDUCE_ATTEMPT_TYPE;
                break;
            case Task:
                attemptType = ExtendConstants.OTHER_ATTEMPT_TYPE;
                break;
            default:
                LOG.error("Check if valid attempt type? " + type);
                throw new ProcessingException("Check if valid attempt type? " + type);
        }

        byte[] taskAttemptIdKeyBytes =
                getAttemptKey(TASK_ATTEMPT_PREFIX, this.jobNumber,
                        eventDetails.getString(ATTEMPTID), attemptType);
        Put pTaskAttempt = new Put(taskAttemptIdKeyBytes);
        pTaskAttempt.add(ExtendConstants.INFO_FAM_BYTES, ExtendConstants.RECORD_TYPE_COL_BYTES,
                Bytes.toBytes(type.toString()));
        iterateAndPreparePuts(eventDetails, pTaskAttempt, recType);
        this.taskPuts.add(pTaskAttempt);
        LOG.debug("add to task list | recType = [ "+recType+" ] | RK = [ "+ Bytes.toStringBinary(taskAttemptIdKeyBytes)+" ]");
    }

    /**
     * Sets the job ID and strips out the job number (job ID minus the "job_" prefix).
     * @param id
     */
    private void setJobId(String id) {
        if (id != null && id.startsWith("job_") && id.length() > 4) {
            this.jobNumber = id.substring(4);
        }
    }

    /**
     * maintains compatibility between hadoop 1.0 keys and hadoop 2.0 keys. It also confirms that this
     * key exists in JobHistoryKeys enum
     * @throws IllegalArgumentException NullPointerException
     */
    private String getKey(String key) throws IllegalArgumentException {
        String checkKey =
                JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.containsKey(key) ? JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING
                        .get(key) : key;
        return (JobHistoryKeys.valueOf(checkKey).toString());
    }

    /**
     * populates a put for long values
     * @param {@link Put} p
     * @param {@link Constants} family
     * @param String key
     * @param long value
     */
    private void populatePut(Put p, byte[] family, String key, long value) {

        byte[] valueBytes;
        valueBytes = (value != 0L) ? Bytes.toBytes(value) : ExtendConstants.ZERO_LONG_BYTES;
        byte[] qualifier = Bytes.toBytes(getKey(key).toLowerCase());
        p.add(family, qualifier, valueBytes);
    }

    /**
     * gets the int values as ints or longs some keys in 2.0 are now int, they were longs in 1.0 this
     * will maintain compatiblity between 1.0 and 2.0 by casting those ints to long
     *
     * keeping this function package level visible (unit testing)
     * @throws IllegalArgumentException if new key is encountered
     */
    byte[] getValue(String key, int value) {
        byte[] valueBytes;
        Class<?> clazz = JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.valueOf(key));
        if (clazz == null) {
            throw new IllegalArgumentException(" unknown key " + key + " encountered while parsing "
                    + this.jobKey);
        }
        if (Long.class.equals(clazz)) {
            valueBytes = (value != 0L) ? Bytes.toBytes(new Long(value)) : ExtendConstants.ZERO_LONG_BYTES;
        } else {
            valueBytes = (value != 0) ? Bytes.toBytes(value) : ExtendConstants.ZERO_INT_BYTES;
        }
        return valueBytes;
    }

    /**
     * populates a put for int values
     * @param {@link Put} p
     * @param {@link Constants} family
     * @param String key
     * @param int value
     */
    private void populatePut(Put p, byte[] family, String key, int value) {

        String jobHistoryKey = getKey(key);
        byte[] valueBytes = getValue(jobHistoryKey, value);
        byte[] qualifier = Bytes.toBytes(jobHistoryKey.toLowerCase());
        p.add(family, qualifier, valueBytes);
    }

    /**
     * populates a put for string values
     * @param {@link Put} p
     * @param {@link Constants} family
     * @param {@link String} key
     * @param String value
     */
    private void populatePut(Put p, byte[] family, String key, String value) {
        byte[] valueBytes;
        valueBytes = Bytes.toBytes(value);
        byte[] qualifier = Bytes.toBytes(getKey(key).toLowerCase());
        p.add(family, qualifier, valueBytes);
    }

    /**
     * populates a put for {@link Counters}
     * @param {@link Put} p
     * @param {@link Constants} family
     * @param String key
     * @param String groupName
     * @param String counterName
     * @param long counterValue
     */
    private void populatePut(Put p, byte[] family, String key, String groupName, String counterName,
                             Long counterValue) {
        byte[] counterPrefix = null;

        try {
            switch (JobHistoryKeys.valueOf(JobHistoryKeys.class, key)) {
                case COUNTERS:
                case TOTAL_COUNTERS:
                case TASK_COUNTERS:
                case TASK_ATTEMPT_COUNTERS:
                    counterPrefix = Bytes.add(ExtendConstants.COUNTER_COLUMN_PREFIX_BYTES, ExtendConstants.SEP_BYTES);
                    break;
                case MAP_COUNTERS:
                    counterPrefix = Bytes.add(ExtendConstants.MAP_COUNTER_COLUMN_PREFIX_BYTES, ExtendConstants.SEP_BYTES);
                    break;
                case REDUCE_COUNTERS:
                    counterPrefix =
                            Bytes.add(ExtendConstants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, ExtendConstants.SEP_BYTES);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown counter type " + key);
            }
        } catch (IllegalArgumentException iae) {
            throw new ProcessingException("Unknown counter type " + key, iae);
        } catch (NullPointerException npe) {
            throw new ProcessingException("Null counter type " + key, npe);
        }

        byte[] groupPrefix = Bytes.add(counterPrefix, Bytes.toBytes(groupName), ExtendConstants.SEP_BYTES);
        byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counterName));

        /**
         * correct and populate map and reduce slot millis
         */
        if ((ExtendConstants.SLOTS_MILLIS_MAPS.equals(counterName)) ||
                (ExtendConstants.SLOTS_MILLIS_REDUCES.equals(counterName))) {
            counterValue = getStandardizedCounterValue(counterName, counterValue);
        }

        p.add(family, qualifier, Bytes.toBytes(counterValue));

    }

    private long getMemoryMb(String key) {
        long memoryMb = 0L;
        if (ExtendConstants.MAP_MEMORY_MB_CONF_KEY.equals(key)){
            memoryMb =  this.jobConf.getLong(key, ExtendConstants.DEFAULT_MAP_MEMORY_MB);
        }else if (ExtendConstants.REDUCE_MEMORY_MB_CONF_KEY.equals(key)){
            memoryMb = this.jobConf.getLong(key, ExtendConstants.DEFAULT_REDUCE_MEMORY_MB);
        }
        if (memoryMb == 0L) {
            throw new ProcessingException(
                    "While correcting slot millis, " + key + " was found to be 0 ");
        }
        return memoryMb;
    }

    /**
     * Issue #51 in hraven on github
     * map and reduce slot millis in Hadoop 2.0 are not calculated properly.
     * They are aproximately 4X off by actual value.
     * calculate the correct map slot millis as
     * hadoop2ReportedMapSlotMillis * yarn.scheduler.minimum-allocation-mb
     *        / mapreduce.mapreduce.memory.mb
     * similarly for reduce slot millis
     * @param counterName
     * @param counterValue
     * @return corrected counter value
     */
    private Long getStandardizedCounterValue(String counterName, Long counterValue) {
        if (jobConf == null) {
            throw new ProcessingException("While correcting slot millis, jobConf is null");
        }
        long yarnSchedulerMinMB = this.jobConf.getLong(ExtendConstants.YARN_SCHEDULER_MIN_MB,
                ExtendConstants.DEFAULT_YARN_SCHEDULER_MIN_MB);
        long updatedCounterValue;
        long memoryMb;
        String key;
        if (ExtendConstants.SLOTS_MILLIS_MAPS.equals(counterName)) {
            key = ExtendConstants.MAP_MEMORY_MB_CONF_KEY;
            memoryMb = getMemoryMb(key);
            updatedCounterValue = counterValue * yarnSchedulerMinMB / memoryMb;
            this.mapSlotMillis = updatedCounterValue;
        } else {
            key = ExtendConstants.REDUCE_MEMORY_MB_CONF_KEY;
            memoryMb = getMemoryMb(key);
            updatedCounterValue = counterValue * yarnSchedulerMinMB / memoryMb;
            this.reduceSlotMillis = updatedCounterValue;
        }

        LOG.info("Updated " + counterName + " from " + counterValue + " to " + updatedCounterValue
                + " based on " + ExtendConstants.YARN_SCHEDULER_MIN_MB + ": " + yarnSchedulerMinMB
                + " and " + key + ": " + memoryMb);
        return updatedCounterValue;
    }

    public byte[] getAttemptKey(String prefix, String jobNumber, String fullId, String attempt_type){
        StringBuilder sb1 = new StringBuilder();
        if (fullId == null) {
            sb1.append("");
        } else {
            StringBuilder sb2 = new StringBuilder();
            sb2.append(prefix).append(jobNumber).append("_");
            if ((fullId.startsWith(sb2.toString())) && (fullId.length() > sb2.length())) {
                sb1.append("a").append(attempt_type).append(ExtendConstants.SEP).append(fullId.substring(sb2.length()));
            }
        }
        return taskKeyConv.toBytes(new TaskKey(this.jobKey, sb1.toString()));
    }

    public byte[] getNewTaskKey(String prefix, String jobNumber, String fullId){
        StringBuilder sb1 = new StringBuilder();
        if (fullId == null) {
            sb1.append("");
        } else {
            StringBuilder sb2 = new StringBuilder();
            sb1.append(prefix).append(jobNumber).append("_");
            if ((fullId.startsWith(sb1.toString())) && (fullId.length() > sb1.length())) {
                sb2.append(fullId.substring(sb1.length()));
                sb1.setLength(0);
                sb1.append("t").append(sb2.charAt(0)).append(ExtendConstants.SEP).append(sb2);
            }
        }
        return taskKeyConv.toBytes(new TaskKey(this.jobKey, sb1.toString()));
    }

    /**
     * Returns the AM Attempt id stripped of the leading job ID, appended to the job row key.
     */
    public byte[] getAMKey(String prefix, String fullId) {

        String taskComponent = prefix + fullId;
        return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Put> getJobPuts() {
        return jobPuts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Put> getTaskPuts() {
        return taskPuts;
    }


    /**
     * utitlity function for printing all puts

    public void printAllPuts(List<Put> p) {
        for (Put p1 : p) {
            Map<byte[], List<KeyValue>> d = p1.getFamilyMap();
            for (byte[] k : d.keySet()) {
                System.out.println(" k " + Bytes.toString(k));
            }
            for (List<KeyValue> lkv : d.values()) {
                for (KeyValue kv : lkv) {
                    System.out.println("\n row: " + taskKeyConv.fromBytes(kv.getRow())
                            + "\n " + Bytes.toString(kv.getQualifier()) + ": " + Bytes.toString(kv.getValue()));
                }
            }
        }
    }
     */

    @Override
    @Deprecated
    public Long getMegaByteMillis() {
        /**
         * calculate mega byte millis puts as:
         * if not uberized:
         *        map slot millis * mapreduce.map.memory.mb
         *        + reduce slot millis * mapreduce.reduce.memory.mb
         *        + yarn.app.mapreduce.am.resource.mb * job runtime
         * if uberized:
         *        yarn.app.mapreduce.am.resource.mb * job run time
         */
        return 0L;
    }

    /**
     * TODO: use new version of JobHistoryFileParser
     * getTobDetails() is not used in hraven-standalone,
     * we return null in getTobDetails() temporarily
     * */
    @Override
    public JobDetails getJobDetails() {
        return null;
    }

    public long getSU(){
        if (endTime == ExtendConstants.NOTFOUND_VALUE || startTime == ExtendConstants.NOTFOUND_VALUE)
        {
            throw new ProcessingException("Cannot calculate megabytemillis for " + jobKey
                    + " since one or more of endTime " + endTime + " startTime " + startTime
                    + " not found!");
        }
        long wall_clock_time = endTime - startTime;
        long core_num = mapAttemptMap.size() + reduceAttemptMap.size() +1 ;
        return  wall_clock_time * core_num *1;
    }


    public long getSU(long cpc){
        long total_su = 0L;

        if (endTime == ExtendConstants.NOTFOUND_VALUE || startTime == ExtendConstants.NOTFOUND_VALUE)
        {
            throw new ProcessingException("Cannot calculate megabytemillis for " + jobKey
                    + " since one or more of endTime " + endTime + " startTime " + startTime
                    + " not found!");
        }

        // AM SU
        long jobRunTime = endTime - startTime;
        total_su = total_su + jobRunTime *cpc;
        LOG.debug("AM = " + "{"+startTime + ", "+endTime+"}");

        // mapper SU
        total_su += calculateSU(mapAttemptMap,cpc);

        // calculate reducer SU
        total_su += calculateSU(reduceAttemptMap,cpc);

        return total_su;
    }

    private long calculateSU(Map<String, DurationPair> map, long cpc){
        long total =0L;
        for (Object o : map.entrySet()) {
            Map.Entry<String, DurationPair> entry = (Map.Entry) o;
            LOG.debug(entry.getKey() + " = " + entry.getValue());

            long end = entry.getValue().getEnd();
            long fail = entry.getValue().getFail();

            if(end > 0 && fail >0){
                throw new ProcessingException("task attempt has both normal finish time and failure finish time");
            }

            long interval = (end > fail) ?
                    (entry.getValue().getEnd() - entry.getValue().getStart()):
                    (entry.getValue().getFail() - entry.getValue().getStart());
            total = total + cpc * interval;
        }
        return total;
    }


    public static long getFinishTimeMillisFromJobHistory(byte[] jobHistoryRaw) {
        long finishTimeMillis = 0;
        if (null == jobHistoryRaw) {
            return finishTimeMillis;
        }

        HadoopVersion hv = JobHistoryFileParserFactory.getVersion(jobHistoryRaw);
        switch (hv) {
            case TWO:
                // three cases (JOB_FINISHED, JOB_KILLED, JOB_FAILED) result in finishTime,
                int startIndex = -1;
                if(startIndex == -1)  // check JOB_FINISHED
                    startIndex = ByteUtil.indexOf(jobHistoryRaw, ExtendConstants.JOB_FINISHED_EVENT_BYTES, 0);
                if(startIndex == -1) // check JOB_KILLED
                    startIndex = ByteUtil.indexOf(jobHistoryRaw, ExtendConstants.JOB_KILLED_EVENT_BYTES, 0);
                if(startIndex == -1) // check JOB_FAILED
                    startIndex = ByteUtil.indexOf(jobHistoryRaw, ExtendConstants.JOB_FAILED_EVENT_BYTES, 0);

                if (startIndex != -1) {
                    // now look for the submit time in this event
                    int secondQuoteIndex =
                            ByteUtil.indexOf(jobHistoryRaw, ExtendConstants.FINISHED_TIME_PREFIX_HADOOP2_BYTES, startIndex);
                    if (secondQuoteIndex != -1) {
                        // read the string that contains the unix timestamp
                        String finishTimeMillisString = Bytes.toString(jobHistoryRaw,
                                secondQuoteIndex + Constants.EPOCH_TIMESTAMP_STRING_LENGTH,
                                Constants.EPOCH_TIMESTAMP_STRING_LENGTH);
                        try {
                            finishTimeMillis = Long.parseLong(finishTimeMillisString);
                        } catch (NumberFormatException nfe) {
                            LOG.error(" caught NFE during conversion of submit time " + finishTimeMillisString + " " + nfe.getMessage());
                            finishTimeMillis = 0;
                        }
                    }
                }
                break;

            case ONE:
            default:
                LOG.error("fetch Job finish time from history is not supported in hadoop version 1");
                break;
        }
        return finishTimeMillis;
    }

    private class DurationPair{
        private long start = 0L;
        private long end = 0L;
        private long failend =0L;

        public DurationPair(long s, long e, long f){
            this.start = s;
            this.end =e;
            this.failend =f;
        }

        public  DurationPair(long s, long e){
            this.start = s;
            this.end =e;
            this.failend = 0L;
        }

        public void setStart(long s){this.start = s;}
        public void setEnd(long e){this.end = e;}
        public void setFail(long f){this.failend =f;}
        public long getStart(){return this.start;}
        public long getEnd(){return this.end;}
        public long getFail(){return this.failend;}

        @Override
        public String toString() {
            return "{" + start +  ", "+end + ", "+ failend +"}";
        }
    }

}