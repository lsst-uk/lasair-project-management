import os, sys
import math
import numpy as np
import time
import ephem
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import settings
from mag import dc_mag

import mysql.connector

def getobjs(msl, ncand):
    cursor   = msl.cursor(buffered=True, dictionary=True)

    query = 'SELECT distinct objectId FROM candidates '
    query += 'WHERE dc_mag_r02 = 0.0 LIMIT %d' % ncand

    cursor.execute(query)
    objlist = []
    for row in cursor:
        objlist.append(row['objectId'])
    return objlist

def makemeans_obj(msl, objectId):
    query = 'SELECT candid, fid, jd, dc_mag from candidates WHERE objectId="%s" ORDER BY jd' % objectId
#    print(query)
    cursor   = msl.cursor(buffered=True, dictionary=True)
    cursor2  = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    oldgjd = oldrjd = 0
    g02 = g08 = g28 = 0
    r02 = r08 = r28 = 0
    n = 0
    for row in cursor:
        jd = row['jd']
        dc_mag = row['dc_mag']
        if row['fid'] == 1:
            f02 = math.exp(-(jd-oldgjd)/2.0)
            f08 = math.exp(-(jd-oldgjd)/8.0)
            f28 = math.exp(-(jd-oldgjd)/28.0)
            g02 = g02*f02 + dc_mag*(1-f02)
            g08 = g08*f08 + dc_mag*(1-f08)
            g28 = g28*f28 + dc_mag*(1-f28)
            oldgjd = jd
            query = 'UPDATE candidates SET dc_mag_r02=-1, dc_mag_r08=-1, dc_mag_r28=-1,dc_mag_g02=%f, dc_mag_g08=%f, dc_mag_g28=%f' % (g02, g08, g28)
        else:
            f02 = math.exp(-(jd-oldrjd)/2.0)
            f08 = math.exp(-(jd-oldrjd)/8.0)
            f28 = math.exp(-(jd-oldrjd)/28.0)
            r02 = r02*f02 + dc_mag*(1-f02)
            r08 = r08*f08 + dc_mag*(1-f08)
            r28 = r28*f28 + dc_mag*(1-f28)
            oldrjd = jd
            query = 'UPDATE candidates SET dc_mag_g02=-1, dc_mag_g08=-1, dc_mag_g28=-1,dc_mag_r02=%f, dc_mag_r08=%f, dc_mag_r28=%f' % (r02, r08, r28)
        query += ' WHERE candid=%d' % row['candid']
        cursor2.execute(query)
#        print(objectId, jd, row['fid'], dc_mag, g02, g08, g28,r02,r08,r28)
        n += 1
    msl.commit()
    return n

if __name__ == "__main__":
    config = {
        'user'    : settings.DB_USER_WRITE,
        'password': settings.DB_PASS_WRITE,
        'host'    : settings.DB_HOST,
        'database': 'ztf'
    }
    msl = mysql.connector.connect(**config)

    totncand = 1
    tottotncand = 0
    while totncand > 0:
        t = time.time()
        nobj = 10000
        totncand = 0
        objlist = getobjs(msl, nobj)
#        print(objlist)
        n = len(objlist)
        print('%d objects to be computed' % n)
        for objectId in objlist:
            ncand = makemeans_obj(msl, objectId)
            totncand += ncand
            tottotncand += ncand
        print('%d candidates (total %d) in %.0f seconds' % (totncand, tottotncand, time.time()-t))
