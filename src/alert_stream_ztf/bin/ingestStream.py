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

def insert_sql(alert):
    """ Creates an insert sql statement for insering the canditate info
        Stamps and prv_candidates are discarded
    """
    names = []
    values = []

    for packet in alert:
        names.append('objectId')
        values.append('"' + packet['objectId'] + '"')
        for name,value in packet['candidate'].items():

            # Must not use 'dec' in mysql, so use 'decl' instead
            if name == 'dec': 
                name = 'decl'
                dec = float(value)
            if name == 'ra': 
                ra = float(value)

            names.append(name)
            if isinstance(value, basestring):
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
    candid = 0
    data = msg_text(alert)
    if data:  # Write your condition statement here

        query = insert_sql(data)
        logger.debug(query)

        candid = alert.get('candid')
        logger.info('inserted %d' % candid)

        try:
            cursor = msl.cursor(buffered=True)
            cursor.execute(query)
            msl.commit()
        except mysql.connector.Error as err:
            logger.error("Database insert failed: %s" % str(err))

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
    parser.add_argument('--stampdump',
                        help='Write postage stamp to /stamps',
                        action='store_true')
    parser.add_argument('--avrodump', 
                        help='Write each avro alert to a file in /avros',
                        action='store_true')
    parser.add_argument('--logging', type=str,
                        help='Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL')

#    avrogroup = parser.add_mutually_exclusive_group()
#    avrogroup.add_argument('--decode', dest='avroFlag', action='store_true',
#                           help='Decode from Avro format. (default)')
#    avrogroup.add_argument('--decode-off', dest='avroFlag',
#                           action='store_false',
#                           help='Do not decode from Avro format.')
#    parser.set_defaults(avroFlag=True)

    args = parser.parse_args()
    if args.logging:
        if args.logging == 'DEBUG':   logger.setLevel(logging.DEBUG)
        if args.logging == 'INFO':    logger.setLevel(logging.INFO)
        if args.logging == 'WARNING': logger.setLevel(logging.WARNING)
        if args.logging == 'ERROR':   logger.setLevel(logging.ERROR)
        if args.logging == 'CRITICAL':logger.setLevel(logging.CRITICAL)

    return args

def main(args):
    # Configure consumer connection to Kafka broker
    conf = {'bootstrap.servers': '{}:9092,{}:9093,{}:9094'.format(args.host,args.host,args.host),
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
        streamReader = alertConsumer.AlertConsumer(args.topic, schema_files, **conf)
        streamReader.__enter__()
    except alertConsumer.EopError as e:
        logging.error(e.message)
        sys.exit()

    while True:
        try:
#            msg = streamReader.poll(decode=args.avroFlag)
            msg = streamReader.poll(decode=True)

            if msg is None:
                continue
            else:
                for record in msg:
                    # Apply filter to each alert
                    if args.stampdir:
                        candid = alert_filter(record, msl, '/stamps/' + args.topic)
                    else:
                        candid = alert_filter(record, msl)

        except alertConsumer.EopError as e:
            # Write when reaching end of partition
            logger.error(e.message)
        except IndexError:
            logger.error('%% Data cannot be decoded\n')
        except UnicodeDecodeError:
            logger.error('%% Unexpected data format received\n')
        except KeyboardInterrupt:
            logger.error('%% Aborted by user\n')
            sys.exit()

        if args.avros:
            dir = '/avros/%s' % args.topic
            try:
                os.makedirs(dir)
            except OSError:
                pass
            f = open('%s/%d.avro' % candid, 'w')
            f.write(streamReader.raw_msg())
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
    main(args)
    logger.info("main has terminated")
