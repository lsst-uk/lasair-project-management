import sys
import math
import numpy as np
import settings
import time

# setup database connection
import mysql.connector
config = {
    'user'    : settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

def make_object(objectId, candlist, debug=False):
    ncand = len(candlist)
    if ncand < 3:
        if debug: 
            print('object %s has too few (%d) candidates, exiting' % (objectId, ncand))
        query = 'DELETE FROM objects WHERE objectId="%s"' % objectId
        if debug: print(query)
        cursor  = msl.cursor(buffered=True, dictionary=True)
        cursor.execute(query)
        msl.commit()
        return -1

    ra = []
    dec = []
    magg = []
    magr = []
    jd   = []
    latestgmag = latestrmag = 99
    for cand in candlist:
        ra.append(cand['ra'])
        dec.append(cand['decl'])
        jd.append(cand['jd'])
        if cand['fid'] == 1: 
            magg.append(cand['magpsf'])
            latestgmag = cand['magpsf']
        else:
            magr.append(cand['magpsf'])
            latestrmag = cand['magpsf']

    if len(magg) > 0:
        maggmin = np.min(magg)
        maggmax = np.max(magg)
        maggmean = np.mean(magg)
        maggmedian = np.median(magg)
    else:
        maggmin = maggmax = maggmean = maggmedian = 99

    if len(magr) > 0:
        magrmin = np.min(magr)
        magrmax = np.max(magr)
        magrmean = np.mean(magr)
        magrmedian = np.median(magr)
    else:
        magrmin = magrmax = magrmean = magrmedian = 99

    attributes  = 'objectId, ncand, stale,'
    attributes += 'ramean, rastd, decmean, decstd,'
    attributes += 'maggmin, maggmax, maggmedian, maggmean,'
    attributes += 'magrmin, magrmax, magrmedian, magrmean,'
    attributes += 'latestgmag, latestrmag, jdmin, jdmax'

    values  = '"%s", %d, 0, ' % (objectId, ncand)
    values += '%.7f, %.3f, %.7f, %.3f, ' % (np.mean(ra), 3600*np.std(ra), np.mean(dec), 3600*np.std(dec))
    values += '%.3f, %.3f, %.3f, %.3f, ' % (maggmin, maggmax, maggmedian, maggmean)
    values += '%.3f, %.3f, %.3f, %.3f, ' % (magrmin, magrmax, magrmedian, magrmean)
    values += '%.3f, %.3f, %.5f, %.5f '  % (latestgmag, latestrmag, np.min(jd), np.max(jd))
    
    query = 'REPLACE INTO objects (' + attributes + ') VALUES (' + values + ')'
    if debug: print(query)
    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    msl.commit()
    return 1

def update_objects(new = False, debug=False):
    t = time.time()
    cursor  = msl.cursor(buffered=True, dictionary=True)
    n = 0
    k = 0
    nbatch = 500000
    oldObjectId = ''
    candlist = []
    ntotalcand = nupdate = ndelete = 0
    while(1):
        if new:
            query =  'SELECT objectId,ra,decl,jd,fid,magpsf FROM candidates '
            query += 'ORDER BY objectId LIMIT %d OFFSET %d' % (nbatch, k*nbatch)
        else:
            query =  'SELECT objectId,ra,decl,jd,fid,magpsf FROM candidates NATURAL JOIN objects '
            query += 'WHERE stale=1 ORDER BY objectId LIMIT %d OFFSET %d' % (nbatch, k*nbatch)
        if debug:
            print(query)
        cursor.execute(query)
        ncand = 0
        for cand in cursor:
            ntotalcand += 1
            ncand += 1
            objectId = cand['objectId']
            if oldObjectId == '':    # the first record
                oldObjectId = objectId
            if objectId == oldObjectId:  # same again
                candlist.append(cand)
            else:
                result = make_object(oldObjectId, candlist)
                if result > 0: nupdate += 1
                else:          ndelete += 1
                candlist = []
                oldObjectId = objectId
        if oldObjectId != '':
            result = make_object(objectId, candlist)
            if result > 0: nupdate += 1
            else:          ndelete += 1
        k += 1
        if debug:
            print('Iteration %d: %d candidates, %d updated objects, %d deleted objects' % (k, ntotalcand, nupdate, ndelete))
        if new:
            if ncand < nbatch:
                 break
        else:
            query = 'SELECT COUNT(*) AS nobj FROM objects WHERE stale=1'
            cursor.execute(query)
            for record in cursor:
                break
            nobj = record['nobj']
            print('%d objects still stale' % nobj)
            if nobj == 0:
                break

    print('-------------- UPDATE OBJECTS --------------')
    print('%d candidates, %d updated objects, %d deleted objects' % (ntotalcand, nupdate, ndelete))
    print('Time %.1f seconds' % (time.time() - t))

if __name__ == "__main__":
    update_objects(new=False, debug=True)
