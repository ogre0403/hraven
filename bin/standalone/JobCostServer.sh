#!/bin/bash
#
# Copyright 2013 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Run on the daemon node per specific cluster
# Usage ./jobFilePreprocessor.sh [hadoopconfdir]
#   [historyrawdir] [historyprocessingdir] [cluster] [batchsize]

#if [ $# -ne 2 ]
#then
#  echo "Usage: `basename $0` [history_dir] [cluster]"
#  exit 1
#fi

source $(dirname $0)/hraven-etl-env.sh

#export HADOOP_HEAPSIZE=4000
myscriptname=$(basename "$0" .sh)
pidfile=$HRAVEN_PID_DIR/$myscriptname.pid
#stopfile=$HRAVEN_PID_DIR/$myscriptname.stop

#if [ -f $stopfile ]; then
#  echo "Error: not allowed to run. Remove $stopfile continue." 1>&2
#  exit 1
#fi

#create_pidfile $HRAVEN_PID_DIR
#trap 'cleanup_pidfile_and_exit $HRAVEN_PID_DIR' INT TERM EXIT


hadoop_rotate_log ()
{
  log=$1;
  num=5;
  if [ -n "$2" ]; then
    num=$2
  fi
  if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
      prev=`expr $num - 1`
      [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
      num=$prev
    done
    mv "$log" "$log.$num";
  fi
}



etl=`dirname "$0"`
etl=`cd "$etl">/dev/null; pwd`
CLASSPATH=`hbase classpath`
HRAVEN_HOME=${HRAVEN_HOME:-$etl/../../}
startStop=$1

# Add libs to CLASSPATH
for f in $HRAVEN_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH};
done

if [ ! -d "$HRAVEN_HOME/logs" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
    mkdir -p $HRAVEN_HOME/logs
fi

log=$HRAVEN_HOME/logs/log.out

case $startStop in
    (start)
    if [ -f $pidfile ]; then
      if kill -0 `cat $pidfile` > /dev/null 2>&1; then
        echo running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    hadoop_rotate_log $log
    echo starting and logging to $log
    nohup java -cp $HRAVEN_HOME/conf/:$CLASSPATH org.nchc.history.Main -d > $log 2>&1 & 
    echo $! > $pidfile
    ;;

  (stop)
    if [ -f $pidfile ]; then
        TARGET_PID=`cat $pidfile`
        kill $TARGET_PID > /dev/null 2>&1  
        rm $pidfile
    else
      echo no process to stop
    fi
    ;;

  (*)
    echo "./JobCostServer.sh [start|stop]"
    exit 1
    ;;
esac



#nohup java -cp $HRAVEN_HOME/conf/:$CLASSPATH org.nchc.history.Main -d > $HRAVEN_HOME/logs/logs.out 2>&1 & echo $! > $pidfile

