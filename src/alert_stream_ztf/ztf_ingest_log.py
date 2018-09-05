#!/home/roy/anaconda2/bin/python
from subprocess import Popen, PIPE
import logging
import date_nid
import time

nid  = date_nid.nid_now()
date = date_nid.nid_to_date(nid)
topic  = 'ztf_' + date + '_programid1'

logger = logging.getLogger('ztf_ingestion')
fh = logging.FileHandler('/data/ztf/logs/' + topic + '.log')
formatter = logging.Formatter( '%(asctime)s|%(levelname)s|%(message)s', '%d/%m/%Y %H:%M:%S')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.setLevel(logging.INFO)

while 1:
    process = Popen(['/home/roy/lasair/src/alert_stream_ztf/ztf_ingest.py'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    logger.info(stdout)
    logger.info(stderr)
    logger.info("waiting 10 minutes ...")
    time.sleep(600)
