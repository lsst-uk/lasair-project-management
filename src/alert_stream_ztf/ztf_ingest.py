import os,sys
from common import settings
from common import date_nid

if len(sys.argv) > 1:
    nid = int(sys.argv[1])
else:
    nid  = date_nid.nid_now()

date = date_nid.nid_to_date(nid)
topic  = 'ztf_' + date + '_programid1'

print('--------------- INGEST FROM KAFKA ------------')
os.system('date')
print("Topic is %s, nid is %d" % (topic, nid))

cmd =  settings.LASAIR_ROOT + 'anaconda3/envs/lasair/bin/python bin/ingestStreamThreaded.py '
cmd += '--logging INFO '
cmd += '--stampdump %d ' % nid
#cmd += '--avrodump '
cmd += '--maxalert %d ' % settings.KAFKA_MAXALERTS
cmd += '--nthread %d '  % settings.KAFKA_THREADS
cmd += '--group %s '    % settings.GROUPID
cmd += '--host %s '     % settings.KAFKA_PRODUCER
cmd += '--topic ' + topic

print(cmd)
os.system(cmd)

tail = 'tail -2 /data/ztf/logs/' + topic + '.log'
os.system(tail)
os.system('date')

py = settings.LASAIR_ROOT + 'anaconda3/envs/lasair/bin/python '

#cmd = py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/coverage.py'
#os.system(cmd)
#os.system('date')

cmd = py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/check_status.py %d' % nid
os.system(cmd)
os.system('date')

#cmd = py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/jpg_stamps.py /data/ztf/stamps/fits/%d /data/ztf/stamps/jpg/%d' % (nid, nid)
#os.system(cmd)
#os.system('date')

cmd = py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/update_objects.py'
os.system(cmd)
os.system('date')

cmd = py + settings.LASAIR_ROOT + '/lasair/src/post_ingest/run_sherlock.py'
os.system(cmd)
os.system('date')

cmd = py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/run_active_queries.py'
os.system(cmd)
os.system('date')

cmd = py + settings.LASAIR_ROOT + 'lasair/src/post_ingest/get_number_candidates.py'
os.system(cmd)
os.system('date')
