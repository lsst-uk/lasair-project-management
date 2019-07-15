import sys
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import settings
import date_nid


from subprocess import Popen, PIPE
import time


while 1:
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    topic  = 'ztf_' + date + '_programid1'
    fh = open('/data/ztf/logs/' + topic + '.log', 'a')

    process = Popen(['/home/roy/anaconda3/envs/lasair/bin/python', '/home/roy/lasair/src/alert_stream_ztf/ztf_ingest.py'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    stdout = stdout.decode('utf-8')
    fh.write(stdout)
    stderr = stderr.decode('utf-8')
    fh.write(stderr)

    fh.write("waiting %d seconds ..." % settings.INGEST_WAIT_TIME)
    fh.close()
    time.sleep(settings.INGEST_WAIT_TIME)
