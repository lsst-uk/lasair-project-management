import sys
import math

from common import settings

sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common/htm/python2')
import htmCircle

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

import dateutil.parser as dp
def jd_from_iso(date):
    if not date.endswith('Z'):
        date += 'Z'
    parsed_t = dp.parse(date)
    unix = int(parsed_t.strftime('%s'))
    jd = unix/86400 + 2440587.5
    return jd

# setup database connection
import mysql.connector
config = {
    'user'    :settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST,
    'database': 'ztf'
}

def run_tns_crossmatch(radius):
# runs the crossmatch of TNS with all the objects
    msl = mysql.connector.connect(**config)
    cursor  = msl.cursor(buffered=True, dictionary=True)

# get user id of user lasair, named "Lasair Bot"
    already_commented = {}
    query = 'SELECT * FROM comments WHERE user=%d' % settings.LASAIR_USERID
    cursor.execute(query)
    for row in cursor:
        already_commented[row['objectId']] = row['content']
#    print("%d comments already from Lasair Bot" % len(already_commented))
    cursor.close()
    msl.close()

    msl = mysql.connector.connect(**config)
    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor2 = msl.cursor(buffered=True, dictionary=True)
    cursor3 = msl.cursor(buffered=True, dictionary=True)
    n_tns = 0
    n_hits = 0
    n_newhits = 0
    # get all the cones and run them
    query = 'SELECT tns_prefix, tns_name, z, type, ra,decl, disc_date, host_name, discovering_groups FROM crossmatch_tns'
    cursor.execute(query)
    for row in cursor:
        tns_name  = row['tns_name']
        type      = row['type']
        z         = row['z']
        tns_prefix  = row['tns_prefix']
        disc_date = row['disc_date']
        host_name = row['host_name']
        myRA      = row['ra']
        myDecl    = row['decl']
        discover = row['discovering_groups']
        n_tns += 1

        iso = str(disc_date).replace(' ', 'T')
# ancient supernovae breaks the date functions!
        if iso.startswith('1604'): continue
        if iso.startswith('1572'): continue
        if iso.startswith('10'): continue
        jd = jd_from_iso(iso)
        mjd = jd - 2400000.5
    
        subClause = htmCircle.htmCircleRegion(16, myRA, myDecl, radius)
        subClause = subClause.replace('htm16ID', 'htm16')
        query2 = 'SELECT * FROM objects WHERE htm16 ' + subClause[14: -2]
#        print(query2)
        cursor2.execute(query2)
        for row in cursor2:
            objectId = row['objectId']
            arcsec = 3600*distance(myRA, myDecl, row['ramean'], row['decmean'])
            if arcsec > radius:
                continue
            n_hits += 1
#            content = 'In TNS as <a href=https://wis-tns.weizmann.ac.il/object/%s>%s%s</a> at %.1f arcsec, discovered %s (MJD %.2f) by %s' % (tns_name, tns_prefix, tns_name, arcsec, disc_date, mjd, discover)

            content = 'In TNS as <a href=https://wis-tns.weizmann.ac.il/object/%s>%s%s</a> (%.1f arcsec separation from ZTF coordinates), discovered (MJD %.2f): '
            content = content % (tns_name, tns_prefix, tns_name, arcsec, mjd)

            if discover: content += 'by %s. ' % discover
            if type:     content += 'Classified as a %s. ' % type
            if z:        content += 'At a redshift z = %s. ' % z

            if objectId in already_commented:
                if already_commented[objectId] == content:
                    continue
                else:
                    query3 = 'DELETE from comments where user=%s and objectId="%s"' % (settings.LASAIR_USERID, objectId)
                    cursor3.execute(query3)
            n_newhits += 1
            query3 = 'INSERT INTO comments (user, objectId, content) '
            query3 += 'VALUES (%d, "%s", "%s")' % (settings.LASAIR_USERID, objectId, content)
#            print(query3)
            cursor3.execute(query3)
            msl.commit()
    print("%d entries in TNS, %d hits in ZTF (%d new)" % (n_tns, n_hits, n_newhits))
    return

if __name__ == "__main__":
    radius = 3  # arcseconds
    run_tns_crossmatch(radius)
