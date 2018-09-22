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
    for cand in candlist:
        ra.append(cand['ra'])
        dec.append(cand['decl'])
        jd.append(cand['jd'])
        if cand['fid'] == 1: magg.append(cand['magpsf'])
        else:                magr.append(cand['magpsf'])
        latestmag = cand['magpsf']

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
    attributes += 'latestmag, jdmin, jdmax'

    values  = '"%s", %d, 0, ' % (objectId, ncand)
    values += '%.3f, %.3f, %.3f, %.3f, ' % (np.mean(ra), np.std(ra), np.mean(dec), np.std(dec))
    values += '%.3f, %.3f, %.3f, %.3f, ' % (maggmin, maggmax, maggmedian, maggmean)
    values += '%.3f, %.3f, %.3f, %.3f, ' % (magrmin, magrmax, magrmedian, magrmean)
    values += '%.3f, %.3f, %.3f '        % (latestmag, np.min(jd), np.max(jd))
    
    query = 'REPLACE INTO objects (' + attributes + ') VALUES (' + values + ')'
    if debug: print(query)
    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    msl.commit()
    return 1

def update_objects(debug=False):
    t = time.time()
    cursor  = msl.cursor(buffered=True, dictionary=True)
    n = 0
    k = 0
    nbatch = 500000
    oldObjectId = ''
    candlist = []
    ntotalcand = nupdate = ndelete = 0
    while(1):
        ncand = 0
        query =  'SELECT objectId,ra,decl,jd,fid,magpsf FROM candidates NATURAL JOIN objects '
        query += 'WHERE stale=1 ORDER BY objectId LIMIT %d OFFSET %d' % (nbatch, k*nbatch)
        if debug:
            print(query)
        cursor.execute(query)
        for cand in cursor:
            ncand += 1
            ntotalcand += 1
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
        k += 1
        if debug:
            print('Iteration %d: %d candidates, %d updated objects, %d deleted objects' % (k, ntotalcand, nupdate, ndelete))
        if ncand < nbatch:
            break
    if ntotalcand > 0:
        result = make_object(objectId, candlist)
        if result > 0: nupdate += 1
        else:          ndelete += 1
    print('-------------- UPDATE OBJECTS --------------')
    print('%d candidates, %d updated objects, %d deleted objects' % (ntotalcand, nupdate, ndelete))
    print('Time %.1f seconds' % (time.time() - t))

if __name__ == "__main__":
    update_objects()
