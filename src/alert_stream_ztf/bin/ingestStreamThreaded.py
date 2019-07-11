"""Consumes stream for ingesting to database

"""

from __future__ import print_function
import argparse
import sys
import os
import time
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common/htm/python')
import htmCircle
import mysql.connector
import settings

import threading

sys.path.append('python')
from lsst.alert.stream import alertConsumer


# attributes from the ZTF schema that the database knows about
wanted_attributes = [
'objectId', 'jd', 'fid', 'pid', 'diffmaglim', 'pdiffimfilename', 'programpi',
'programid', 'candid', 'isdiffpos', 'tblid', 'nid', 'rcid', 'field', 'xpos',
'ypos', 'ra', 'decl', 'magpsf', 'sigmapsf', 'chipsf', 'magap', 'sigmagap', 'distnr',
'magnr', 'sigmagnr', 'chinr', 'sharpnr', 'sky', 'magdiff', 'fwhm', 'classtar', 'mindtoedge',
'magfromlim', 'seeratio', 'aimage', 'bimage', 'aimagerat', 'bimagerat', 'elong', 'nneg',
'nbad', 'rb', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'sumrat', 'magapbig', 'sigmagapbig',
'ranr', 'decnr', 'sgmag1', 'srmag1', 'simag1', 'szmag1', 'sgscore1', 'distpsnr1', 'ndethist',
'ncovhist', 'jdstarthist', 'jdendhist', 'scorr', 'tooflag', 'objectidps1', 'objectidps2',
'sgmag2', 'srmag2', 'simag2', 'szmag2', 'sgscore2', 'distpsnr2', 'objectidps3', 'sgmag3',
'srmag3', 'simag3', 'szmag3', 'sgscore3', 'distpsnr3', 'nmtchps', 'rfid', 'jdstartref',
'jdendref', 'nframesref', 
'rbversion', 'dsnrms', 'ssnrms', 'dsdiff', 'magzpsci', 'magzpsciunc', 'magzpscirms',
'nmatches', 'clrcoeff', 'clrcounc', 'zpclrcov', 'zpmed', 'clrmed', 'clrrms', 'neargaia',
'neargaiabright', 'maggaia', 'maggaiabright', 'exptime',
'htmid16']


# which tables to use
candidates    = 'candidates'
objects       = 'objects'
noncandidates = 'noncandidates'
#candidates    = 'candidates_test'
#objects       = 'objects_test'
#noncandidates = 'noncandidates_test'

# Configure Avro reader schema
schema_files = ['ztf-avro-alert/schema/candidate.avsc',
                'ztf-avro-alert/schema/cutout.avsc',
                'ztf-avro-alert/schema/prv_candidate.avsc',
                'ztf-avro-alert/schema/alert.avsc']

def insert_sql_candidate(candidate, objectId, times):
    """ Creates an insert sql statement for insering the canditate info
        Also works foe candidates in the prv
    """
    names = []
    values = []

    names.append('objectId')
    values.append('"' + objectId + '"')
    for name,value in candidate.items():

        # Must not use 'dec' in mysql, so use 'decl' instead
        if name == 'dec': 
            name = 'decl'
            dec = float(value)
        if name == 'ra': 
            ra = float(value)

        if name in wanted_attributes:
            names.append(name)
            if isinstance(value, str):
                values.append('"' + value + '"')
            elif name.startswith('ss') and not value:
                values.append('-999.0')
            else:
                values.append(str(value))

# Compute the HTM ID for later cone searches
    try:
        htmID = htmCircle.htmID(16, ra, dec)
    except:
        times['log'] += ('Cannot get HTMID for ra=%f, dec=%f' % (ra, dec))

    names.append('htmid16')
    values.append(str(htmID))

# and here is the SQL
    sql = 'INSERT IGNORE INTO %s \n(%s) \nVALUES \n(%s)' % (candidates, ','.join(names), ','.join(values))
    return {'sql': sql, 'times':times}

def msg_text(message):
    """Remove postage stamp cutouts from an alert message.
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text

def write_stamp_file(stamp_dict, output_dir, times):
    """Given a stamp dict that follows the cutout schema,
       write data to a file in a given directory.
    """
    try:
        filename = stamp_dict['fileName']
        try:
            os.makedirs(output_dir)
        except OSError:
            pass
        out_path = os.path.join(output_dir, filename)
        with open(out_path, 'wb') as f:
            f.write(stamp_dict['stampData'])
    except TypeError:
        times['log'] += ('%% Cannot get stamp\n')
    return times

def insert_candidate(msl, candidate, objectId, stalefile, times):
    """ gets the SQL for insertion, then inserts the candidate
        and makes the ojects stale
    """
# insert the candidate record
    d = insert_sql_candidate(candidate, objectId, times)
    query = d['sql']
    times = d['times']
    t = time.time()
    try:
        cursor = msl.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()
    except mysql.connector.Error as err:
        times['log'] += ('Database insert candidate failed: %s' % str(err))

    try:
        stalefile.write('%s\n' % objectId)
    except:
        times['log'] += ('Stalefile write failed: %s' % objectId)
    
    msl.commit()

    times['insert'] += time.time() - t
    return times

def alert_filter(alert, msl, times, stalefile, stampdir=None):
    """Filter to apply to each alert.
       See schemas: https://github.com/ZwickyTransientFacility/ztf-avro-alert
    """
    candid = 0
    data = msg_text(alert)
    if data:  # Write your condition statement here

        objectId = data['objectId']
        candid   = data['candid']

# look for non detection limiting magnitude
        prv_array = data['prv_candidates']
        if prv_array:
            noncanlist = []
            query4 = ''
            for prv in prv_array:
                if prv['candid']:
                    if prv['magpsf']:
                        times = insert_candidate(msl, prv, objectId, stalefile, times)
#                        print('%s %s' % (objectId, str(prv['candid'])))
                else:
                    jd         = prv['jd']
                    fid        = prv['fid']
                    diffmaglim = prv['diffmaglim']
                    noncanlist.append('("%s", %.5f, %d, %.3f)' % (objectId, jd, fid, diffmaglim))
            if len(noncanlist) > 0:
                t = time.time()
                query4 = 'INSERT INTO %s (objectId, jd, fid, diffmaglim) VALUES ' % noncandidates
                query4 += ', '.join(noncanlist)
#                logger.debug(query4)
                try:
                    cursor = msl.cursor(buffered=True)
                    cursor.execute(query4)
                    cursor.close()
                except mysql.connector.Error as err:
#                    logger.debug('Noncandidate insert failed: %s' % str(err))
                    pass
                times['insert'] += time.time() - t

        times = insert_candidate(msl, data['candidate'], objectId, stalefile, times)

        t = time.time()
        if stampdir:  # Collect all postage stamps
            times = write_stamp_file( alert.get('cutoutDifference'), stampdir, times)
            times = write_stamp_file( alert.get('cutoutTemplate'),   stampdir, times)
            times = write_stamp_file( alert.get('cutoutScience'),    stampdir, times)
        times['stamp'] += time.time() - t

        return {'candid': candid, 'times':times}

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', type=str,
                        help='Hostname or IP of Kafka host to connect to.')
    parser.add_argument('--topic', type=str,
                        help='Name of Kafka topic to listen to.')
    parser.add_argument('--group', type=str,
                        help='Globally unique name of the consumer group. '
                        'Consumers in the same group will share messages '
                        '(i.e., only one consumer will receive a message, '
                        'as in a queue). Default is value of $HOSTNAME.')
    parser.add_argument('--frombeginning', 
                         help='Start from the beginning of the topic',
                         action='store_true')
    parser.add_argument('--stampdump',
                        help='Write postage stamp to /stamps/<dir>')
    parser.add_argument('--avrodump', 
                        help='Write each avro alert to a file in /avros',
                        action='store_true')
    parser.add_argument('--logging', type=str,
                        help='Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL')
    parser.add_argument('--maxalert', type=int,
                        help='Max alerts to be fetched per thread')
    parser.add_argument('--nthread', type=int,
                        help='Number of threads to use')

    args = parser.parse_args()

    return args


class Consumer(threading.Thread):
    def __init__(self, threadID, args, conf, times):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.args = args
        self.times = times

    def run(self):
        # Configure database connection
        msl = mysql.connector.connect(
            user     = settings.DB_USER_WRITE, 
            password = settings.DB_PASS_WRITE, 
            host     = settings.DB_HOST, 
            database = settings.DB_DATABASE,
            )
    
        # Start consumer and print alert stream
        
        try:
            streamReader = alertConsumer.AlertConsumer(self.args.topic, self.args.frombeginning, schema_files, **self.conf)
            streamReader.__enter__()
        except alertConsumer.EopError as e:
            self.times['log'] += '%d: %s\n' % (self.threadID, e.message)
            sys.exit()
    
        if self.args.maxalert:
            maxalert = self.args.maxalert
        else:
            maxalert = 50000
    
        stalefile = open('/data/ztf/stale/file%02d'%self.threadID, 'w')
        nalert = 0
        while nalert < maxalert:
            t = time.time()
            try:
                msg = streamReader.poll(decode=True, timeout=settings.KAFKA_TIMEOUT)
            except alertConsumer.EopError as e:
                break

            self.times['fetch'] += time.time() - t

            if msg is None:
                break
            else:
                for record in msg:
                    # Apply filter to each alert
                    if self.args.stampdump:
                        d = alert_filter(record, msl, self.times, stalefile, '/data/ztf/stamps/fits/' + self.args.stampdump)
                    else:
                        d = alert_filter(record, msl, self.times, stalefile)
                    candid = d['candid']
                    self.times = d['times']
                    nalert += 1
    
        self.times['log'] += '%d: finished with %d alerts\n' % (self.threadID, nalert)
        if self.args.avrodump:
            dir = '/data/ztf/avros/%s' % self.args.topic
            try:
                os.makedirs(dir)
            except OSError:
                pass
            f = open('%s/%d.avro' % (dir, candid), 'wb')
            f.write(streamReader.raw_msg)
            f.close()

        streamReader.__exit__(0,0,0)
        stalefile.close()
        return self.times

def main():
    args = parse_args()

    # Configure consumer connection to Kafka broker
#    print('Connecting to Kafka at %s' % args.host)
#    conf = {'bootstrap.servers': '{}:9092,{}:9093,{}:9094'.format(args.host,args.host,args.host),
#            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    conf = {'bootstrap.servers': '{}:9092'.format(args.host,args.host,args.host),
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    if args.group: conf['group.id'] = args.group
    else:          conf['group.id'] = 'LASAIR'

    print('Configuration = %s' % str(conf))

    if args.nthread:
        nthread = args.nthread
    else:
        nthread = 1
    print('Threads = %d' % nthread)

    os.system('rm -f /data/ztf/stale/*')

    timeses = []
    for i in range(nthread):
        timeses.append({'insert':0.0, 'stamp': 0.0, 'fetch':0.0, 'log':''})

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, args, conf, timeses[t]))
    
    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

    time_total = time.time() - t
    print('\n  Insert  Stamp   Fetch   Log')
    for t in range(nthread):
        ti = timeses[t]
        print('%7.1f %7.1f %7.1f %s' % (ti['insert'], ti['stamp'], ti['fetch'], ti['log'].strip()))
    print('Run time %f' % time_total)

if __name__ == '__main__':
    main()
