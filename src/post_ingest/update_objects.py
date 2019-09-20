import os, sys
import math
import numpy as np
import time
import ephem
from common import settings
sys.path.append(settings.LASAIR_ROOT + 'lasair/src/alert_stream_ztf/common/htm/python')
import htmCircle
import threading

# setup database connection
import mysql.connector

def make_ema(candlist, cursor):
    oldgjd = oldrjd = 0
    g02 = g08 = g28 = 0
    r02 = r08 = r28 = 0
    dc_mag_g = dc_mag_r = 0
    n = 0
    for row in candlist:
        jd = row['jd']
        dc_mag = row['dc_mag']
        if row['fid'] == 1:
            f02 = math.exp(-(jd-oldgjd)/2.0)
            f08 = math.exp(-(jd-oldgjd)/8.0)
            f28 = math.exp(-(jd-oldgjd)/28.0)
            dc_mag_g = dc_mag
            g02 = g02*f02 + dc_mag*(1-f02)
            g08 = g08*f08 + dc_mag*(1-f08)
            g28 = g28*f28 + dc_mag*(1-f28)
            oldgjd = jd
            query = 'UPDATE candidates SET dc_mag_r02=-1, dc_mag_r08=-1, dc_mag_r28=-1,dc_mag_g02=%f, dc_mag_g08=%f, dc_mag_g28=%f' % (g02, g08, g28)
        else:
            f02 = math.exp(-(jd-oldrjd)/2.0)
            f08 = math.exp(-(jd-oldrjd)/8.0)
            f28 = math.exp(-(jd-oldrjd)/28.0)
            dc_mag_r = dc_mag
            r02 = r02*f02 + dc_mag*(1-f02)
            r08 = r08*f08 + dc_mag*(1-f08)
            r28 = r28*f28 + dc_mag*(1-f28)
            oldrjd = jd
            query = 'UPDATE candidates SET dc_mag_g02=-1, dc_mag_g08=-1, dc_mag_g28=-1,dc_mag_r02=%f, dc_mag_r08=%f, dc_mag_r28=%f' % (r02, r08, r28)
        query += ' WHERE candid=%d' % row['candid']
        if row['dc_mag_g02'] == 0.0:
            cursor.execute(query)
            n += 1
    d = {'n':n, 'dc_mag_g':dc_mag_g, 'g02':g02, 'g08':g08, 'g28':g28, 'dc_mag_r':dc_mag_r, 'r02':r02, 'r08':r08, 'r28':g28}
    return d

def make_object(objectId, candlist, msl):
    cursor  = msl.cursor(buffered=True, dictionary=True)
    d = make_ema(candlist, cursor)
    ema_updates = d['n']

    ncand = len(candlist)
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

        sgmag1    = cand['sgmag1']
        srmag1    = cand['srmag1']
        sgscore1  = cand['sgscore1']
        distpsnr1 = cand['distpsnr1']
        if not sgmag1: sgmag1 = 'NULL'
        if not srmag1: srmag1 = 'NULL'
        if not sgscore1: sgscore1 = 'NULL'
        if not distpsnr1: distpsnr1 = 'NULL'

    if len(jd) == 0:
        return 0

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
    sets['sgmag1']     = sgmag1
    sets['srmag1']     = srmag1
    sets['sgscore1']   = sgscore1
    sets['distpsnr1']  = distpsnr1
    sets['htm16']      = htm16

    sets['latest_dc_mag_g']   = d['dc_mag_g']
    sets['latest_dc_mag_g02'] = d['g02']
    sets['latest_dc_mag_g08'] = d['g08']
    sets['latest_dc_mag_g28'] = d['g28']

    sets['latest_dc_mag_r']   = d['dc_mag_r']
    sets['latest_dc_mag_r02'] = d['r02']
    sets['latest_dc_mag_r08'] = d['r08']
    sets['latest_dc_mag_r28'] = d['r28']

    list = []
    query = 'UPDATE objects SET '
    for key,value in sets.items():
        list.append(key + '=' + str(value))
    query += ', '.join(list)
    query += ' WHERE objectId="' + objectId + '"'
    try:
        cursor.execute(query)
    except:
        print('Update object failed. Query was:\n', query)
    return ema_updates

#    print('%s updated %d candidates' % (objectId, ema_updates))
    msl.commit()
    return ema_updates

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
        ntotalcand = nupdate = nema = 0
        cursor   = msl.cursor(buffered=True, dictionary=True)
        cursor2  = msl.cursor(buffered=True, dictionary=True)
        for objectId in self.objectIds:
            query = 'SELECT candid, objectId,ra,decl,jd,fid,magpsf'
            query += ',dc_mag,dc_mag_g02,dc_mag_g08,dc_mag_g28,dc_mag_r02,dc_mag_r08,dc_mag_r28'
            query += ',sgmag1, srmag1, sgscore1, distpsnr1'
            query += ' FROM candidates WHERE objectId="%s" ORDER BY jd'        
            query = query % objectId
            cursor.execute(query)
            candlist = []
            for cand in cursor:
                candlist.append(cand)
            ntotalcand += len(candlist)
    
            query2 = 'INSERT IGNORE INTO objects (objectId) VALUES ("%s")' % objectId
            cursor2.execute(query2)
            ema_updates = make_object(objectId, candlist, msl)
            nema += ema_updates
            nupdate += 1
    
        self.times['log'] += ('%d candidates, %d updated objects, %d updates ema' % (ntotalcand, nupdate, nema))
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
    print('------------ UPDATE OBJECTS --------------')
    t = time.time()

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
    print('Update objects finished in %.1f sec' % (time.time() - t))

