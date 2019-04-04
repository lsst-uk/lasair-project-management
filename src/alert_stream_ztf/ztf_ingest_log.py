#!/home/roy/anaconda2/bin/python

import sys
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')


from subprocess import Popen, PIPE
import logging
import date_nid
import time


while 1:
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    topic  = 'ztf_' + date + '_programid1'
    logger = logging.getLogger('ztf_ingestion')
    fh = logging.FileHandler('/data/ztf/logs/' + topic + '.log')
    formatter = logging.Formatter( '%(asctime)s|%(levelname)s|%(message)s', '%d/%m/%Y %H:%M:%S')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    process = Popen(['/home/roy/anaconda3/bin/python', '/home/roy/lasair/src/alert_stream_ztf/ztf_ingest.py'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    logger.info(stdout)
    logger.info(stderr)
    logger.info("waiting 10 minutes ...")
    logger.removeHandler(fh)
    fh.close()
    logging.shutdown()
    time.sleep(600)
