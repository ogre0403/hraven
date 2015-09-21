#!/usr/bin/python

import requests
import json
import datetime
import time
import sys
import os

WEB_URL = "https://140.110.141.58:8443/"
CLUSTER = "NCHC"
REST_URL= WEB_URL + "api/v1/alluser/"+CLUSTER+"/?start=%s&end=%s"
#ROOT_LOG_DIR = "/tmp"
ROOT_LOG_DIR = "/home/acctadm1/qdaily"

# generate in checkInput()
MONTH_LOG_DIR = ""
LOG_DAILY_DETAIL = ""
LOG_DAILY_SUMMARY = ""
QUERY_DATE = ""

def main():
    url = checkInput()
    response = requests.get(url,verify=False)
    j = response.text
    #Convert JSON string into Python nested dictionary/list.
    r = json.loads(j)

    writer1 = os.fdopen(os.open(LOG_DAILY_DETAIL, os.O_WRONLY | os.O_CREAT, 0600), 'w')
    writer2 = os.fdopen(os.open(LOG_DAILY_SUMMARY, os.O_WRONLY | os.O_CREAT, 0600), 'w')
    outputLog(r,writer1, writer2)
    writer1.close()
    writer2.close()

def outputLog(response, writer1, writer2):
    count = 0
    total = 0
    for key, value in response.iteritems():
        for item in value:
            output = [None] * 14
            output[0] = key             #login-name
            output[1] = item["queue"]   #q-name
            # HIVE job name may have line break
            output[2] = " ".join(item["jobName"].split()) #job-name
            output[3] = item["jobId"]   #job-id
            output[4] = getDateString(item["launchDate"]/1000)  #job-submit-date
            output[5] = getDateString(item["finishTime"]/1000)  #job-end-date
            output[6] = "P"             #job-type
            output[7] = str(item["finishedMaps"])       #min-cpu (number of map)
            output[8] = str(item["finishedReduces"])    #max-cpu (number of reduce)
            output[9] = "1"                             #real-cpu ( set to 1)
            output[10] = "0"                            #waiting-time
            output[11] = str(item["megabyteMillis"]/1000) #wall-clock-time
            output[12] = str(item["megabyteMillis"]/1000) #cpu-time
            output[13] = "0"
            print ":".join(output)
            count = count +1
            total = total + item["megabyteMillis"]/1000
            writer1.write(":".join(output)+"\n")

    summary = [None] * 5
    summary[0] = QUERY_DATE
    summary[1] = "0"
    summary[2] = "0"
    summary[3] = str(count)
    summary[4] = str(total)
    writer2.write(":".join(summary)+"\n")


def getDateString(ts):
    return datetime.datetime.fromtimestamp(ts).strftime('%Y/%m/%d %H/%M/%S')

def getTimestamp(YMD):
    a = YMD +" 00:00:00"
    timeArray = time.strptime(a, "%Y%m%d %H:%M:%S")
    return int(time.mktime(timeArray))*1000


def checkInput():
    global MONTH_LOG_DIR
    global LOG_DAILY_DETAIL
    global LOG_DAILY_SUMMARY
    global QUERY_DATE

    if (len(sys.argv) == 1):
        # start date is not specified, query yesterday
        end = datetime.date.today()
        start = end - datetime.timedelta(days=1)
    elif (len(sys.argv) == 2):
        # end date is not specified, query start_date and next day
        start = datetime.datetime.strptime(sys.argv[1],"%Y%m%d")
        end = start +  datetime.timedelta(days=1)
    else:
        usage()
        exit()

    QUERY_DATE = start.strftime("%Y/%m/%d")
    MONTH_LOG_DIR =  ROOT_LOG_DIR + os.sep + start.strftime("%Y") + os.sep  + start.strftime("%m")
    if not os.path.exists( MONTH_LOG_DIR):
        mkdir_p(MONTH_LOG_DIR)
    LOG_DAILY_DETAIL= MONTH_LOG_DIR + os.sep + "dbqacct." + start.strftime("%Y%m%d")
    LOG_DAILY_SUMMARY = MONTH_LOG_DIR + os.sep + "dbqacct.log." + start.strftime("%Y%m%d")
    return REST_URL % (getTimestamp(start.strftime('%Y%m%d')),getTimestamp(end.strftime('%Y%m%d')))

def usage():
    print "."+os.sep+ os.path.basename(__file__) + " [start_date] "

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

if __name__ == "__main__":
    main()
