import sys
import math
import numpy as np
import time
import ephem
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import settings

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
    latestgmag = latestrmag = 'NULL'
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
        maggmin = maggmax = maggmean = maggmedian = 'NULL'

    if len(magr) > 0:
        magrmin = np.min(magr)
        magrmax = np.max(magr)
        magrmean = np.mean(magr)
        magrmedian = np.median(magr)
    else:
        magrmin = magrmax = magrmean = magrmedian = 'NULL'

    ramean  = np.mean(ra)
    decmean = np.mean(dec)
    ce = ephem.Equatorial(math.radians(ramean), math.radians(decmean))
    cg = ephem.Galactic(ce)
    glonmean = math.degrees(float(repr(cg.lon)))
    glatmean = math.degrees(float(repr(cg.lat)))

    sets = {}
    sets['ncand']      = ncand
    sets['stale']      = 0
    sets['ramean']     = ramean
    sets['rastd']      = 3600*np.std(ra)
    sets['decmean']    = decmean
    sets['decstd']     = 3600*np.std(dec)
    sets['maggmin']    = maggmin
    sets['maggmax']    = maggmax
    sets['maggmedian'] = maggmedian
    sets['maggmean']   = maggmean
    sets['magrmin']    = magrmin
    sets['magrmax']    = magrmax
    sets['magrmedian'] = magrmedian
    sets['magrmean']   = magrmean
    sets['latestgmag'] = latestgmag
    sets['latestrmag'] = latestrmag
    sets['jdmin']      = np.min(jd)
    sets['jdmax']      = np.max(jd)
    sets['glatmean']   = glatmean
    sets['glonmean']   = glonmean

    list = []
    query = 'UPDATE objects SET '
    for key,value in sets.items():
        list.append(key + '=' + str(value))
    query += ', '.join(list)
    query += ' WHERE objectId="' + objectId + '"'

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
#        if new:
#            query =  'SELECT objectId,ra,decl,jd,fid,magpsf FROM candidates '
#            query += 'ORDER BY objectId LIMIT %d OFFSET %d' % (nbatch, k*nbatch)
#        else:
        query =  'SELECT objectId,ra,decl,jd,fid,magpsf FROM candidates NATURAL JOIN objects '
        query += 'WHERE stale=1 ORDER BY objectId LIMIT %d ' % nbatch
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
        if debug:
            print('Iteration %d: %d candidates, %d updated objects, %d deleted objects' % (k, ntotalcand, nupdate, ndelete))
#        if new:
#            if ncand < nbatch:
#                 break
#        else:
        query = 'SELECT COUNT(*) AS nobj FROM objects WHERE stale=1'
        cursor.execute(query)
        for record in cursor:
            break
        nobj = record['nobj']
        if nobj == 1:
            result = make_object(oldObjectId, candlist)
            nobj -= 1

        print('%d objects still stale' % nobj)
        if nobj == 0:
            break

    print('-------------- UPDATE OBJECTS --------------')
    print('%d candidates, %d updated objects, %d deleted objects' % (ntotalcand, nupdate, ndelete))
    print('Time %.1f seconds' % (time.time() - t))

if __name__ == "__main__":
    update_objects(debug=True)
