"""Consumes stream for ingesting to database

"""

from __future__ import print_function
import argparse
import sys
import os
import time
from common import settings
sys.path.append(settings.LASAIR_ROOT + 'lasair/src/alert_stream_ztf/common/htm/python')
import htmCircle
import mysql.connector
from mag import dc_mag

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
'neargaiabright', 'maggaia', 'maggaiabright', 'exptime', 'drb', 'drbversion',
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

def insert_sql_candidate(candidate, objectId):
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

        if name == 'fid':       fid = int(value)
        if name == 'magpsf':    magpsf = float(value)
        if name == 'sigmapsf':  sigmapsf = float(value)
        if name == 'magnr':     magnr = float(value)
        if name == 'sigmagnr':  sigmagnr = float(value)
        if name == 'magzpsci':  magzpsci = float(value)
        if name == 'isdiffpos': isdiffpos = value

# Compute the HTM ID for later cone searches
    try:
        htmID = htmCircle.htmID(16, ra, dec)
    except:
        print('Cannot get HTMID for ra=%f, dec=%f' % (ra, dec))

    names.append('htmid16')
    values.append(str(htmID))

# Compute apparent magnitude
    d = dc_mag(fid, magpsf,sigmapsf, magnr,sigmagnr, magzpsci, isdiffpos)
    names.append('dc_mag')
    values.append(str(d['dc_mag']))
    names.append('dc_sigmag')
    values.append(str(d['dc_sigmag']))


# and here is the SQL
    sql = 'INSERT IGNORE INTO %s \n(%s) \nVALUES \n(%s)' % (candidates, ','.join(names), ','.join(values))
    return {'sql': sql}

def msg_text(message):
    """Remove postage stamp cutouts from an alert message.
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text

def write_stamp_file(stamp_dict, output_dir):
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
        print('%% Cannot get stamp\n')
    return

def insert_candidate(msl, candidate, objectId, stalefile):
    """ gets the SQL for insertion, then inserts the candidate
        and makes the ojects stale
    """
# insert the candidate record
    d = insert_sql_candidate(candidate, objectId)
    query = d['sql']
    t = time.time()
    try:
        cursor = msl.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()
    except mysql.connector.Error as err:
        print('Database insert candidate failed: %s' % str(err))

    try:
        stalefile.write('%s\n' % objectId)
    except:
        print('Stalefile write failed: %s' % objectId)
    
    msl.commit()

    return

def alert_filter(alert, msl, stalefile, stampdir=None):
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
                        insert_candidate(msl, prv, objectId, stalefile)
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
                try:
                    cursor = msl.cursor(buffered=True)
                    cursor.execute(query4)
                    cursor.close()
                except mysql.connector.Error as err:
                    pass

        insert_candidate(msl, data['candidate'], objectId, stalefile)

        t = time.time()
        if stampdir:  # Collect all postage stamps
            write_stamp_file( alert.get('cutoutDifference'), stampdir)
            write_stamp_file( alert.get('cutoutTemplate'),   stampdir)
            write_stamp_file( alert.get('cutoutScience'),    stampdir)

        return candid

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
    def __init__(self, threadID, args, conf):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.args = args

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
            print('Cannot start reader: %d: %s\n' % (self.threadID, e.message))
            return
    
        if self.args.maxalert:
            maxalert = self.args.maxalert
        else:
            maxalert = 50000
    
        stalefile = open('/data/ztf/stale/file%02d'%self.threadID, 'w')
        startt = time.time()
        nalert = 0
        while nalert < maxalert:
            t = time.time()
            try:
                msg = streamReader.poll(decode=True, timeout=settings.KAFKA_TIMEOUT)
            except alertConsumer.EopError as e:
                print(self.threadID, e)
                break

            if msg is None:
                print(self.threadID, 'null message')
                break
            else:
                for record in msg:
                    # Apply filter to each alert
                    if self.args.stampdump:
                        candid = alert_filter(record, msl, stalefile, '/data/ztf/stamps/fits/' + self.args.stampdump)
                    else:
                        candid = alert_filter(record, msl, stalefile)
                    nalert += 1
                    if nalert%100 == 0:
                        print('thread %d nalert %d time %.1f' % ((self.threadID, nalert, time.time()-startt)))
    
                    if self.args.avrodump:
                        dir = '/data/ztf/avros/%s' % self.args.topic
                        try:
                            os.makedirs(dir)
                        except OSError:
                            pass
                        f = open('%s/%d.avro' % (dir, candid), 'wb')
                        f.write(streamReader.raw_msg)
                        f.close()

        print('%d: finished with %d alerts\n' % (self.threadID, nalert))

        streamReader.__exit__(0,0,0)
        stalefile.close()

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

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, args, conf))
    
    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

    time_total = time.time() - t
    print('Run time %f' % time_total)

if __name__ == '__main__':
    main()
