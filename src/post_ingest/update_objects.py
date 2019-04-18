import os, sys
import math
import numpy as np
import time
import ephem
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import settings
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common/htm/python')
import htmCircle
import threading

#candidates = 'candidates_test'
#objects    = 'objects_test'
candidates = 'candidates'
objects    = 'objects'

# setup database connection
import mysql.connector

def make_object(objectId, candlist, msl):
    ncand = len(candlist)
    if ncand < 3:
        query = 'DELETE FROM %s WHERE objectId="%s"' % (objects, objectId)
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

# Compute the HTM ID for later cone searches
    try:
        htm16 = htmCircle.htmID(16, ramean, decmean)
    except:
        print('Cannot get HTMID for ra=%f, dec=%f' % (ramean, decmean))

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
    sets['htm16']      = htm16

    list = []
    query = 'UPDATE %s SET ' % objects
    for key,value in sets.items():
        list.append(key + '=' + str(value))
    query += ', '.join(list)
    query += ' WHERE objectId="' + objectId + '"'

    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    msl.commit()
    return 1

class Updater(threading.Thread):
    def __init__(self, threadID, objectIds, times):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.objectIds = objectIds
        self.times = times

    def run(self):
        config = {
            'user'    : settings.DB_USER_WRITE,
            'password': settings.DB_PASS_WRITE,
            'host'    : settings.DB_HOST,
            'database': 'ztf'
        }
        msl = mysql.connector.connect(**config)

        t = time.time()
        ntotalcand = nupdate = ndelete = 0
        cursor   = msl.cursor(buffered=True, dictionary=True)
        cursor2  = msl.cursor(buffered=True, dictionary=True)
        for objectId in self.objectIds:
            query = 'SELECT candid, objectId,ra,decl,jd,fid,magpsf from %s WHERE objectId="%s" ORDER BY jd'        
            query = query % (candidates, objectId)
            cursor.execute(query)
            candlist = []
            for cand in cursor:
                candlist.append(cand)
            ntotalcand += len(candlist)
    
            query2 = 'INSERT IGNORE INTO %s (objectId) VALUES ("%s")' % (objects, objectId)
            cursor2.execute(query2)
            result = make_object(objectId, candlist, msl)
            if result < 0:   # deleted the object
                ndelete += 1
            else:
                nupdate += 1
    
        self.times['log'] += ('%d candidates, %d updated objects, %d deleted objects' % (ntotalcand, nupdate, ndelete))
        self.times['time'] = (time.time() - t)


def splitList(objectsForUpdate, bins = None):

   # Break the list of candidates up into the number of CPUs
    listLength = len(objectsForUpdate)

    nProcessors = bins

    if listLength <= nProcessors:
        nProcessors = listLength

   # Create nProcessors x empty arrays
    listChunks = [ [] for i in range(nProcessors) ]

    i = 0

    for item in objectsForUpdate:
        listChunks[i].append(item)
        i += 1
        if i >= nProcessors:
            i = 0

    return nProcessors, listChunks


if __name__ == "__main__":
    os.system('cd /data/ztf/stale; cat file* | sort | uniq > all_file')
    lines = open('/data/ztf/stale/all_file').readlines()
    objectIds = []
    for line in lines:
        objectIds.append(line.strip())

    nthread = 4
    print('%d threads' % nthread)
    nProcessors, listChunks = splitList(objectIds, bins=nthread)
    nthread = nProcessors

    timeses = []
    for i in range(nthread):
        timeses.append({'time':0.0, 'log':''})

    # make the thread list
    thread_list = []
    for th in range(nthread):
        print('Thread %d starting with %d objectIds' % (th, len(listChunks[th])))
        thread_list.append(Updater(th, listChunks[th], timeses[th]))

    # start them up
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

    for th in range(nthread):
        ti = timeses[th]
        print('Thread %d %7.1f sec %s' % (th, ti['time'], ti['log']))

