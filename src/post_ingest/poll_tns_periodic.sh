#!/bin/bash

# Intermittently poll the TNS for missed objects.

if [ $# -ne 5 ]
then
   echo "Usage: `basename $0` <python> <pythonpath> <codebase> <logfile> <pages>"
   echo "E.g. `basename $0` /home/roy/anaconda3/envs/sherlock/bin/python    /home/roy/lasair/src/alert_stream_ztf/common  /home/roy/lasair /data/ztf/logs/poll_tns_periodic.log 10"
   exit 1
fi

export PYTHON=$1
export PYTHONPATH=$2
export CODEBASE=$3
export LOGFILE=$4
export PAGES=$5

# Don't allow more than 60 pages.
if [ $PAGES -ge 60 ]
then
  export PAGES=60
fi

echo '=============================' >> $LOGFILE
echo date >> $LOGFILE
for ((i=0;i<PAGES;i++)); do
  $PYTHON $CODEBASE/src/post_ingest/poll_tns.py --pageSize=500 --pageNumber=$i >> $LOGFILE 2>&1
done

