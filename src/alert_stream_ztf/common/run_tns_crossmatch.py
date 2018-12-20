import sys
import math
import settings

sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common/htm/python')
import htmCircle

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

# setup database connection
import mysql.connector
config = {
    'user'    :settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

def run_tns_crossmatch(radius):
# runs the crossmatch of TNS with all the objects
    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor2 = msl.cursor(buffered=True, dictionary=True)
    cursor3 = msl.cursor(buffered=True, dictionary=True)

# get user id of user lasair, named "Lasair Bot"
    already_commented = []
    query = 'SELECT * FROM comments WHERE user=%d' % settings.LASAIR_USERID
    cursor.execute(query)
    for row in cursor:
        already_commented.append(row['objectId'])
#    print("%d comments already from Lasair Bot" % len(already_commented))

    n_tns = 0
    n_hits = 0
    n_newhits = 0
    # get all the cones and run them
    query = 'SELECT tns_name,ra,decl,disc_date FROM crossmatch_tns'
    cursor.execute(query)
    for row in cursor:
        tns_name  = row['tns_name']
        disc_date = row['disc_date']
        myRA      = row['ra']
        myDecl    = row['decl']
        n_tns += 1
    
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
            if objectId in already_commented:
                continue
            n_newhits += 1
            comment = 'In TNS as <a href=https://wis-tns.weizmann.ac.il/object/%s>%s</a> discovered %s' % (tns_name, tns_name, disc_date)
            query3 = 'INSERT INTO comments (user, objectId, content) '
            query3 += 'VALUES (%d, "%s", "%s")' % (settings.LASAIR_USERID, objectId, comment)
#            print(query3)
            cursor3.execute(query3)
            msl.commit()
    print("%d entries in TNS, %d hits in ZTF (%d new)" % (n_tns, n_hits, n_newhits))
    return

if __name__ == "__main__":
    radius = 3  # arcseconds
    run_tns_crossmatch(radius)
