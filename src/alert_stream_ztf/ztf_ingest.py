#!/home/roy/anaconda2/bin/python
# This runs in a crontab. Works out todays date to make the topic
# Sucks in the alerts and puts them in the database
# Converts the FITS to jpegs in the "post ingest" phase
# crontab entry is
# 0 15 * * * /home/roy/lasair/src/alert_stream_ztf/ztf_ingest.py

import os
from time import gmtime, strftime
from datetime import date

g = gmtime()
topic  = 'ztf_' + strftime("%Y%m%d", g) + '_programid1'

d0 = date(2017, 1, 1)
d1 = date(g.tm_year, g.tm_mon, g.tm_mday)
nid = (d1 - d0).days
print "Topic is %s, nid is %d" % (topic, nid)


cmd = 'docker run --rm '
cmd += '--mount type=bind,source=/data/ztf/logs,target=/logs '
cmd += '--mount type=bind,source=/data/ztf/avros,target=/avros '
cmd += '--mount type=bind,source=/data/ztf/stamps/fits,target=/stamps '
cmd += 'ztf-listener python bin/ingestStream.py '
cmd += '--logging INFO '
cmd += '--stampdump %d ' % nid
cmd += '--group LASAIR '
cmd += '--host public.alerts.ztf.uw.edu '
cmd += '--topic ' + topic

os.system('date')
os.system(cmd)

tail = 'tail -2 /data/ztf/logs/' + topic + '.log'
os.system(tail)

cmd = 'mkdir /data/ztf/stamps/jpg/%d; /home/roy/anaconda2/bin/python lasair/src/post_ingest/jpg_stamps.py /data/ztf/stamps/fits/%d /data/ztf/stamps/jpg/%d' % (nid, nid, nid)
os.system(cmd)
os.system('date')
