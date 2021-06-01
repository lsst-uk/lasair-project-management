#!/usr/bin/env python
"""Get the TNS list.  Max size of request is 1000 rows.

Usage:
  %s [--pageSize=<n>] [--pageNumber=<n>] [--inLastNumberOfDays=<n>] [--userId=<userId>] [--userName=<userName>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --pageSize=<n>               Request page size [default: 50].
  --pageNumber=<n>             Request page number [default: 0].
  --inLastNumberOfDays=<n>     Get everything in the specified number of days.
  --userId=<userId>            Use this userId for authorized access to TNS.
  --userName=<userName>        Use this userName for authorized access to TNS.

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
#import MySQLdb
import mysql.connector as MySQLdb
from gkutils import Struct, cleanOptions, dbConnect, coords_sex_to_dec, floatValue, intValue, nullValue
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common/htm/python2')
import requests
import csv
import htmCircle
sys.path.append('/home/roy/lasair/src/alert_stream_ztf')
from common import settings

def getTNSRow(conn, tnsName):
   """
   Has the TNS row been updated compared with what's in the database?
   """

   try:
      #cursor = conn.cursor (MySQLdb.cursors.DictCursor)
      cursor = conn.cursor (dictionary=True)

      cursor.execute ("""
           select tns_prefix, tns_name
             from crossmatch_tns
            where tns_name = %s
      """, (tnsName,))
      resultSet = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet


def deleteTNSRow(conn, tnsName):
    try:
        #cursor = conn.cursor (MySQLdb.cursors.DictCursor)
        cursor = conn.cursor (dictionary=True)

        cursor.execute ("""
            delete from crossmatch_tns
            where tns_name = %s
            """, (tnsName,))

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    cursor.close ()
    conn.commit()
    return


def insertTNS(conn, tnsEntry):

    insertId = 0
    try:
        #cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor = conn.cursor (dictionary=True)

        cursor.execute ("""
            insert into crossmatch_tns (
               ra,
               decl,
               tns_prefix,
               tns_name,
               z,
               hostz,
               host_name,
               disc_mag,
               disc_mag_filter,
               disc_instruments,
               type,
               public,
               sender,
               associated_groups,
               classifying_groups,
               discovering_groups,
               class_instrument,
               tnsid,
               disc_int_name,
               ext_catalogs,
               disc_date,
               tns_at,
               htm16)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (tnsEntry['ra'],
                  tnsEntry['dec'],
                  tnsEntry['prefix'],
                  tnsEntry['suffix'],
                  floatValue(tnsEntry['Redshift']),
                  floatValue(tnsEntry['Host Redshift']),
                  nullValue(tnsEntry['Host Name']),
                  floatValue(tnsEntry['Discovery Mag/Flux']),
                  nullValue(tnsEntry['Discovery Filter']),
                  nullValue(tnsEntry['Disc. Instrument/s']),
                  nullValue(tnsEntry['Obj. Type']),
                  intValue(tnsEntry['Public']),
                  nullValue(tnsEntry['Sender']),
                  nullValue(tnsEntry['Associated Group/s']),
                  nullValue(tnsEntry['Classifying Group/s']),
                  nullValue(tnsEntry['Reporting Group/s']),
                  nullValue(tnsEntry['Class. Instrument/s']),
                  intValue(tnsEntry['ID']),
                  nullValue(tnsEntry['Disc. Internal Name']),
                  nullValue(tnsEntry['Ext. catalog/s']),
                  nullValue(tnsEntry['Discovery Date (UT)']),
                  intValue(tnsEntry['TNS AT']),
                  tnsEntry['htm16']))
        insertId = cursor.lastrowid
        cursor.close ()

    except MySQLdb.Error as e:
        if e[0] == 1142: # Can't insert - don't have permission
            print("Can't insert.  User doesn't have permission.")
        else:
            print(e)

    #insertId = conn.insert_id()
    conn.commit()
    return insertId


def pollTNS(page=0, resultSize=50, inLastNumberDays=None, header = None):

    from datetime import datetime, date, time, timedelta
    if inLastNumberDays:
        pastTime = datetime.now() - timedelta(days=inLastNumberDays)
        pastTime = pastTime.strftime("%Y-%m-%d")
    else:
        pastTime = ""

    try:
        response = requests.get(
            headers = header,
            url="http://www.wis-tns.org/search",
            params={
                "page": page,
                "name": "",
                "name_like": "0",
                "isTNS_AT": "all",
                "public": "all",
                "unclassified_at": "0",
                "classified_sne": "0",
                "ra": "",
                "decl": "",
                "radius": "",
                "coords_unit": "arcsec",
                "groupid[]": "null",
                "type[]": "null",
                "discoverer": "",
                "date_start[date]": pastTime,
                "date_end[date]": "2100-01-01",
                "discovery_mag_min": "",
                "discovery_mag_max": "",
                "redshift_min": "",
                "redshift_max": "",
                "spectra_count": "",
                "associated_groups[]": "null",
                "display[redshift]": "1",
                "display[hostname]": "1",
                "display[host_redshift]": "1",
                "display[source_group_name]": "1",
                "display[programs_name]": "1",
                "display[isTNS_AT]": "1",
                "display[public]": "1",
                "display[spectra_count]": "1",
                "display[discoverymag]": "1",
                "display[discmagfilter]": "1",
                "display[discoverydate]": "1",
                "display[discoverer]": "1",
                "display[sources]": "1",
                "display[bibcode]": "1",
                "num_page": resultSize,
                "edit[type]": "",
                "edit[objname]": "",
                "edit[id]": "",
                "sort": "desc",
                "order": "discoverydate",
                "format": "csv"
            },
        )

#        content = response.content
        content = response.text
        status_code = response.status_code
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
        sys.exit(0)

    return status_code, content

def getTNSData(opts):
    if type(opts) is dict:
        options = Struct(**opts)
    else:
        options = opts

#    import yaml
#    with open(options.configFile) as yaml_file:
#        config = yaml.load(yaml_file)

    username = settings.DB_USER_WRITE
    password = settings.DB_PASS_WRITE
    hostname = settings.DB_HOST
    database = 'ztf'
#    database = settings.DB

#    username = config['databases']['local']['username']
#    password = config['databases']['local']['password']
#    database = config['databases']['local']['database']
#    hostname = config['databases']['local']['hostname']

    header = None
    if options.userId is not None and options.userName is not None:
        header = {'User-Agent': json.dumps({'tns_marker': {'tns_id': int(options.userId), 'type': 'user', 'name': options.userName}})}

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    inLastNumberOfDays = None
    if options.inLastNumberOfDays:
        inLastNumberOfDays = int(options.inLastNumberOfDays)

    status_code, content = pollTNS(page = int(options.pageNumber), resultSize = int(options.pageSize), inLastNumberDays = inLastNumberOfDays, header = header)

#    csvEntries = csv.DictReader(content.splitlines(), delimiter=',')
#    data = csvEntries
    data = csv.DictReader(content.splitlines(), delimiter=',')
    rowsAdded = 0
    for row in data:
        try:
            name = row['Name'].strip().split()
        except KeyError as e:
            continue

        if len(name) != 2:
            prefix = 'SN'
            suffix = name[0]
        else:
            prefix = name[0]
            suffix = name[1]

        if row['Discovery Date (UT)'].strip() == '0000-00-00 00:00:00':
            # Set the discovery date to January of the suffix name.
            discoveryDate = '%s-01-01 00:00:00' % suffix[0:4]
            row['Discovery Date (UT)'] = discoveryDate

#        if not 'Type' in row:  # sometimes TNS does not supply Type -- RDW
#            row['Type'] = 'null'

        row['prefix'] = prefix
        row['suffix'] = suffix
        ra, dec = coords_sex_to_dec(row['RA'], row['DEC'])
        if ra == 0 and dec == 0:
            print("Cannot store record for %s. No coordinates provided!" % row['Name'].strip())
            continue

        row['ra'] = ra
        row['dec'] = dec
        htm16 = htmCircle.htmID(16, ra, dec)
        row['htm16'] = htm16
        tnsEntry = getTNSRow(conn, suffix)
        if tnsEntry:
            if tnsEntry['tns_prefix'] != prefix:
                # The entry has been updated on TNS - classified! Otherwise do nothing!
                deleteTNSRow(conn, suffix)
                insertTNS(conn, row)
                print("Object %s has been updated" % row['suffix'])
                rowsAdded += 1
        else:
            insertTNS(conn, row)
            print("Object %s has been added" % row['suffix'])
            rowsAdded += 1
        #print prefix, suffix, ra, dec, htm16, row['Discovery Date (UT)']

    print("Total rows added or modified = %d" % rowsAdded)

    conn.commit()
    conn.close()


def main():
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    getTNSData(options)


if __name__ == '__main__':
    main()
