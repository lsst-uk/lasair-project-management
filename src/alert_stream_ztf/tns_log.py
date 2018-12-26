#!/home/roy/anaconda2/bin/python

import sys, os
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import date_nid
import time

while 1:
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    logfile = '/data/ztf/logs/ztf_' + date + '_tns.log'

    cmd = 'date >> %s;' % logfile
    cmd += '/home/roy/anaconda2/bin/python /home/roy/lasair/src/post_ingest/poll_tns.py --pageSize=1000 --inLastNumberOfDays=180 >> %s;' % logfile
    cmd += '/home/roy/anaconda2/bin/python /home/roy/lasair/src/alert_stream_ztf/common/run_tns_crossmatch.py >> %s' % logfile
    os.system(cmd)
    time.sleep(7200)
