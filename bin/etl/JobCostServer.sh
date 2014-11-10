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

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` [history_dir] [cluster]"
  exit 1
fi

source $(dirname $0)/hraven-etl-env.sh

export HADOOP_HEAPSIZE=4000
myscriptname=$(basename "$0" .sh)
stopfile=$HRAVEN_PID_DIR/$myscriptname.stop

if [ -f $stopfile ]; then
  echo "Error: not allowed to run. Remove $stopfile continue." 1>&2
  exit 1
fi

create_pidfile $HRAVEN_PID_DIR
trap 'cleanup_pidfile_and_exit $HRAVEN_PID_DIR' INT TERM EXIT

#hadoop --config $1 jar $hravenEtlJar com.twitter.hraven.etl.JobCostServer -libjars=$LIBJARS -d  -i $2 -c $3
etl=`dirname "$0"`
etl=`cd "$etl">/dev/null; pwd`
CLASSPATH=`hbase classpath`
HRAVEN_HOME=${HRAVEN_HOME:-$etl/../../}

java -cp $HRAVEN_HOME/conf/:$CLASSPATH:$hravenEtlJar:$LIBJARS  com.twitter.hraven.etl.JobCostServer -d  -i $1 -c $2
