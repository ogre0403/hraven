#!/bin/sh

#!/bin/sh
# Small script to setup the HBase tables used by OpenTSDB.

test -n "$HBASE_HOME" || {
  echo >&2 'The environment variable HBASE_HOME must be set'
  exit 1
}
test -d "$HBASE_HOME" || {
  echo >&2 "No such directory: HBASE_HOME=$HBASE_HOME"
  exit 1
}


# HBase scripts also use a variable named `HBASE_HOME', and having this
# variable in the environment with a value somewhat different from what
# they expect can confuse them in some cases.  So rename the variable.
hbh=$HBASE_HOME
unset HBASE_HOME
exec "$hbh/bin/hbase" shell <<EOF

create 'job_history', {NAME => 'i'}
create 'job_history_task', {NAME => 'i'}
create 'job_history-by_jobId', {NAME => 'i'}
create 'job_history_raw', {NAME => 'i', BLOOMFILTER => 'ROWCOL'}, {NAME => 'r', VERSIONS => 1, BLOCKCACHE => false}
create 'job_history_process', {NAME => 'i', VERSIONS => 10}
create 'job_running',{NAME => 'r', TTL => '300'}, {NAME => 'u', TTL => 5184000}
EOF