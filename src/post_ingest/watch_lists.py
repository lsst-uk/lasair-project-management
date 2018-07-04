import sys
import math
import settings

sys.path.append('/home/roy/lasair/src/alert_stream_ztf/utils/htm/python')
import htmCircle

if sys.argv < 1:
    print("usage: python watchlists.py nid")
    sys.exit(1)

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

nid = int(sys.argv[1])
print('nid is %d' % nid)

import mysql.connector
msl = mysql.connector.connect(\
            user    =settings.DB_USER_WRITE, \
            password=settings.DB_PASS_WRITE, \
            host    =settings.DB_HOST, \
            database='ztf')

cursor  = msl.cursor(buffered=True, dictionary=True)
cursor2 = msl.cursor(buffered=True, dictionary=True)
cursor3 = msl.cursor(buffered=True, dictionary=True)

query = 'DROP TABLE today'
cursor.execute(query)

query = 'CREATE TABLE today AS (SELECT objectId,candid,ra,decl,htmid16 FROM candidates WHERE nid=%d)' % nid
cursor.execute(query)

query = 'SELECT userId,myObject,myRA,myDecl,radius FROM watch_positions'
cursor.execute(query)
for watch_pos in cursor:
    userId   = watch_pos['userId']
    myObject = watch_pos['myObject']
    myRA     = watch_pos['myRA']
    myDecl   = watch_pos['myDecl']
    radius_arcsec   = watch_pos['radius']*3600.0

    subClause = htmCircle.htmCircleRegion(16, myRA, myDecl, radius_arcsec)
    subClause = subClause.replace('htm16ID', 'htmid16')
    query2 = 'SELECT * FROM today WHERE htmid16 ' + subClause[15: -2]
#    print(query2)
    cursor2.execute(query2)
    for row in cursor2:
        objectId = row['objectId']
        candid   = row['candid']
        arcsec = 3600*distance(myRA, myDecl, row['ra'], row['decl'])
#        print('User %s object %s gets %s at %.1f arcsec' % (userId, myObject, objectId, arcsec))

        query3 = 'INSERT INTO watch_hits (userId, myObject, nid, arcsec, candid, objectId) '
        query3 += 'VALUES ("%s", "%s", %d, %f, %d, "%s")' % (userId, myObject, nid, arcsec, candid, objectId)
        print(query3)
        cursor3.execute(query3)
        msl.commit()
