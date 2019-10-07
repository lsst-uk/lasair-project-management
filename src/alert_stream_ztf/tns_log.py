import sys, os
import time
from common import date_nid
from common import settings

while 1:
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    logfile = '/data/ztf/logs/ztf_' + date + '_tns.log'

    py = settings.LASAIR_ROOT + 'anaconda3/envs/lasair/bin/python '
    cmd = 'date >> %s;' % logfile
    cmd += py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/poll_tns.py --pageSize=%d --inLastNumberOfDays=%d >> %s;' 
    cmd = cmd % (settings.TNSpageSize, settings.TNSinLastNumberOfDays, logfile)
    cmd += py + settings.LASAIR_ROOT + 'lasair/src/alert_stream_ztf/common/run_tns_crossmatch.py >> %s' % logfile
    os.system(cmd)
    time.sleep(settings.TNSsleepTime)
