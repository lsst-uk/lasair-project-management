#!/usr/bin/env python

"""Consumes stream for ingesting to database

"""

from __future__ import print_function
import argparse
import sys
import os
from lsst.alert.stream import alertConsumer

import htmCircle
import mysql.connector
import settings

import logging
logger = logging.getLogger('ztf_ingestion')

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

import time
time_insert = 0.0
time_stamp  = 0.0
time_fetch  = 0.0

def insert_sql_candidate(alert):
    """ Creates an insert sql statement for insering the canditate info
        Stamps and prv_candidates are discarded
    """
    names = []
    values = []

    names.append('objectId')
    values.append('"' + alert['objectId'] + '"')
    for name,value in alert['candidate'].items():

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
            else:
                values.append(str(value))

# Compute the HTM ID for later cone searches
    try:
        htmID = htmCircle.htmID(16, ra, dec)
    except:
        logger.error('Cannot get HTMID for ra=%f, dec=%f' % (ra, dec))
        return 'failed'

    names.append('htmid16')
    values.append(str(htmID))

# and here is the SQL
    return 'INSERT INTO candidates \n(%s) \nVALUES \n(%s)' % (','.join(names), ','.join(values))

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
        logger.error('%% Cannot get stamp\n')
    return

def alert_filter(alert, msl, stampdir=None):
    """Filter to apply to each alert.
       See schemas: https://github.com/ZwickyTransientFacility/ztf-avro-alert
    """
    global time_insert
    global time_stamp
    candid = 0
    data = msg_text(alert)
    if data:  # Write your condition statement here

        t = time.time()
        objectId = data['objectId']


# look for non detection limiting magnitude
        prv_array = data['prv_candidates']
        if prv_array:
            noncanlist = []
            query4 = ''
            for prv in prv_array:
                if not prv['candid']:
                    jd         = prv['jd']
                    fid        = prv['fid']
                    diffmaglim = prv['diffmaglim']
                    noncanlist.append('("%s", %.5f, %d, %.3f)' % (objectId, jd, fid, diffmaglim))
            if len(noncanlist) > 0:
                query4 = 'INSERT INTO noncandidates (objectId, jd, fid, diffmaglim) VALUES '
                query4 += ', '.join(noncanlist)
                logger.debug(query4)
                try:
                    cursor = msl.cursor(buffered=True)
                    cursor.execute(query4)
                    msl.commit()
                except mysql.connector.Error as err:
                    logger.debug("Noncandidate insert failed: %s" % str(err))

# insert the candidate record
        query  = insert_sql_candidate(data)
# for new objects 
        query2 = 'INSERT IGNORE INTO objects (objectId, stale) VALUES ("%s", 1)' % objectId
# for existing objects
        query3 = 'UPDATE objects set stale=1 where objectId="%s"' % objectId

        logger.debug(query)
        logger.debug(query2)
        logger.debug(query3)
        try:
            cursor = msl.cursor(buffered=True)
            cursor.execute(query)
            cursor.execute(query2)
            cursor.execute(query3)
            msl.commit()
        except mysql.connector.Error as err:
            logger.error("Database insert failed: %s" % str(err))

        candid = alert.get('candid')
        logger.debug('inserted %d' % candid)

        time_insert += time.time() - t

        t = time.time()
        if stampdir:  # Collect all postage stamps
            write_stamp_file( alert.get('cutoutDifference'), stampdir)
            write_stamp_file( alert.get('cutoutTemplate'),   stampdir)
            write_stamp_file( alert.get('cutoutScience'),    stampdir)
        time_stamp += time.time() - t

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

    args = parser.parse_args()
    if args.logging:
        if args.logging == 'DEBUG':   logger.setLevel(logging.DEBUG)
        if args.logging == 'INFO':    logger.setLevel(logging.INFO)
        if args.logging == 'WARNING': logger.setLevel(logging.WARNING)
        if args.logging == 'ERROR':   logger.setLevel(logging.ERROR)
        if args.logging == 'CRITICAL':logger.setLevel(logging.CRITICAL)

    return args

def main(args):
    global time_fetch
    # Configure consumer connection to Kafka broker
    print('Connecting to Kafka at %s' % args.host)
#    conf = {'bootstrap.servers': '{}:9092,{}:9093,{}:9094'.format(args.host,args.host,args.host),
#            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    conf = {'bootstrap.servers': '{}:9092'.format(args.host,args.host,args.host),
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    if args.group:
        conf['group.id'] = args.group
    else:
        conf['group.id'] = 'LASAIR'
#        conf['group.id'] = os.environ['HOSTNAME']

    logger.info('Configuration = %s' % str(conf))

    # Configure Avro reader schema
    schema_files = ["../ztf-avro-alert/schema/candidate.avsc",
                    "../ztf-avro-alert/schema/cutout.avsc",
                    "../ztf-avro-alert/schema/prv_candidate.avsc",
                    "../ztf-avro-alert/schema/alert.avsc"]

    # Configure database connection
    msl = mysql.connector.connect(
        user     = settings.DB_USER, 
        password = settings.DB_PASSWORD, 
        host     = settings.DB_HOST, 
        database = settings.DB_DATABASE)

    # Start consumer and print alert stream
    
    try:
        streamReader = alertConsumer.AlertConsumer(args.topic, args.frombeginning, schema_files, **conf)
        streamReader.__enter__()
    except alertConsumer.EopError as e:
        logging.error(e.message)
        sys.exit()

    nalert = oldnalert = oldoldnalert = 0
    while nalert < 50000:   # run for an hour at 1320 per minute then do post processing
        try:
#            msg = streamReader.poll(decode=args.avroFlag)
            t = time.time()
            msg = streamReader.poll(decode=True)
            time_fetch += time.time() - t

            if msg is None:
                continue
            else:
                for record in msg:
                    # Apply filter to each alert
                    if args.stampdump:
                        candid = alert_filter(record, msl, '/stamps/' + args.stampdump)
                    else:
                        candid = alert_filter(record, msl)
                    nalert += 1

        except alertConsumer.EopError as e:
            # Write when reaching end of partition
            logger.error(e.message)
            if nalert == 0 or (nalert == oldnalert and nalert == oldoldnalert):
                logger.info("Finished stream after %d alerts" % nalert)
                return
            else:
                logger.info("Pausing stream after %d alerts" % nalert)
                time.sleep(10)
                oldoldnalert = oldnalert
                oldnalert = nalert
        except IndexError:
            logger.error('%% Data cannot be decoded\n')
        except UnicodeDecodeError:
            logger.error('%% Unexpected data format received\n')
        except KeyboardInterrupt:
            logger.error('%% Aborted by user\n')
            sys.exit()

        if args.avrodump:
            dir = '/avros/%s' % args.topic
            try:
                os.makedirs(dir)
            except OSError:
                pass
            f = open('%s/%d.avro' % (dir, candid), 'wb')
            f.write(streamReader.raw_msg)
            f.close()
    return


if __name__ == "__main__":
    args = parse_args()
    if args.topic:
        fh = logging.FileHandler('/logs/' + args.topic + '.log')
    else:
        fh = logging.FileHandler('/logs/junk.log')
    formatter = logging.Formatter( '%(asctime)s|%(levelname)s|%(message)s', '%d/%m/%Y %H:%M:%S')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("starting ingestion")
    t = time.time()
    main(args)
    time_total = time.time() - t
    logger.info("main has terminated: insert time %f, stampfile time %f, fetch time %f" % (time_insert, time_stamp, time_fetch))
    logger.info("Run time %f" % time_total)
