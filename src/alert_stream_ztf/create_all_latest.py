import os, sys
import math
import numpy as np
import time
import ephem
import settings
from mag import dc_mag

import mysql.connector

def getobjs(msl, ncand):
    cursor   = msl.cursor(buffered=True, dictionary=True)

    query = 'SELECT objectId FROM objects '
    query += 'WHERE latest_dc_mag_r08 is NULL LIMIT %d' % ncand

    cursor.execute(query)
    objlist = []
    for row in cursor:
        objlist.append(row['objectId'])
    return objlist

def makemeans_obj(msl, objectId):
    query = 'SELECT jd, fid, dc_mag, '
    query += 'dc_mag_g02,dc_mag_g08,dc_mag_g28, dc_mag_r02,dc_mag_r08,dc_mag_r28, '
    query += 'sgmag1, srmag1, sgscore1, distpsnr1 from candidates WHERE objectId="%s" ORDER BY jd' % objectId
#    print(objectId)
    cursor   = msl.cursor(buffered=True, dictionary=True)
    cursor2  = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    oldgjd = oldrjd = 0
    g = r = 0.0
    g02 = g08 = g28 = 0
    r02 = r08 = r28 = 0
    sgmag1 = srmag1 = sgscore1 = distpsnr1 = -1.0
    n = 0
    for row in cursor:
        if row['fid'] == 1:
            g = row['dc_mag']
            g02 = row['dc_mag_g02']
            g08 = row['dc_mag_g08']
            g28 = row['dc_mag_g28']
        else:
            r = row['dc_mag']
            r02 = row['dc_mag_r02']
            r08 = row['dc_mag_r08']
            r28 = row['dc_mag_r28']

        if row['sgmag1']:    sgmag1    = row['sgmag1']
        if row['srmag1']:    srmag1    = row['srmag1']
        if row['sgscore1']:  sgscore1  = row['sgscore1']
        if row['distpsnr1']: distpsnr1 = row['distpsnr1']

        query = 'UPDATE objects SET latest_dc_mag_g=%f, latest_dc_mag_r=%f, ' % (g, r)
        query += 'latest_dc_mag_g02=%f, latest_dc_mag_g08=%f, latest_dc_mag_g28=%f,' % (g02, g08, g28)
        query += 'latest_dc_mag_r02=%f, latest_dc_mag_r08=%f, latest_dc_mag_r28=%f,' % (r02, r08, r28)
        query += 'sgmag1=%f, srmag1=%f, sgscore1=%f, distpsnr1=%f' % (sgmag1, srmag1, sgscore1, distpsnr1)
        query += ' WHERE objectId="%s"' % objectId
#        print(query)
        cursor2.execute(query)
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
        n = len(objlist)
        print('%d objects to be computed' % n)
        for objectId in objlist:
            ncand = makemeans_obj(msl, objectId)
            totncand += ncand
            tottotncand += ncand
        print('%d candidates (total %d) in %.0f seconds' % (totncand, tottotncand, time.time()-t))
