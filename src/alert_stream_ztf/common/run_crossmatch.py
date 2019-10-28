import sys
import math
import settings
sys.path.append(settings.LASAIR_ROOT + 'lasair/src/alert_stream_ztf/common/htm/python')
import htmCircle

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

# setup database connection
import mysql.connector
config = {
    'user'    : settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

def run_watchlist(wl_id, delete_old=True):
# runs the crossmatch of a given watchlist with all the objects
    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor2 = msl.cursor(buffered=True, dictionary=True)
    cursor3 = msl.cursor(buffered=True, dictionary=True)

    # get information about the watchlist we are running
    query = 'SELECT name,radius FROM watchlists WHERE wl_id=%d' % wl_id
    cursor.execute(query)
    for watchlist in cursor:
        wl_name   = watchlist['name']
        wl_radius = watchlist['radius']
#    print ("default radius = %.1f" % wl_radius)
    
    # clean out previous hits
    if delete_old:
        query = 'DELETE FROM watchlist_hits WHERE wl_id=%d' % wl_id
        cursor.execute(query)
        msl.commit()

    # make a list of all the hits to return it
    newhitlist = []
    
    # get all the cones and run them
    query = 'SELECT cone_id,name,ra,decl,radius FROM watchlist_cones WHERE wl_id=%d' % wl_id
    cursor.execute(query)
    ncandidate = 0
    for watch_pos in cursor:
        cone_id  = watch_pos['cone_id']
        name     = watch_pos['name']
        myRA     = watch_pos['ra']
        myDecl   = watch_pos['decl']
        radius   = watch_pos['radius']
    
        if not radius:
            radius = wl_radius
        subClause = htmCircle.htmCircleRegion(16, myRA, myDecl, radius)

        subClause = subClause.replace('htm16ID', 'htm16')
        query2 = 'SELECT * FROM objects WHERE htm16 ' + subClause[14: -2]

#        subClause = subClause.replace('htm16ID', 'htmid16')
#        query2 = 'SELECT objectId,ra,decl,count(*) AS ncand FROM candidates '
#        query2 += 'WHERE htmid16 ' + subClause[15: -2]

#        print(query2)
        cursor2.execute(query2)
        for row in cursor2:
            objectId = row['objectId']
            if not objectId: continue
            ndethist = row['ncand']
#            arcsec = 3600*distance(myRA, myDecl, row['ramean'], row['decmean'])
            arcsec = 3600*distance(myRA, myDecl, row['ramean'], row['decmean'])
            if arcsec > radius:
                continue

            ndethist = row['ncand']
            ncandidate += ndethist
    
            query3 = 'INSERT INTO watchlist_hits (wl_id, cone_id, objectId, ndethist, arcsec, name) '
            query3 += 'VALUES (%d, %d, "%s", %d, %f, "%s")' % (wl_id, cone_id, objectId, ndethist, arcsec, name)
#            print(query3)
            try:
                cursor3.execute(query3)
                msl.commit()
                newhitlist.append({
                    'wl_id':wl_id, 
                    'cone_id':cone_id, 
                    'name':name, 
                    'objectId':objectId, 
                    'ndethist':ndethist, 
                    'arcsec':arcsec
                })
            except mysql.connector.errors.IntegrityError as e:
#                print(e)
                pass  # this objectId is already recorded as a hit for this watchlist
    return {'newhitlist':newhitlist, 'ncandidate':ncandidate}

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("usage: python run_crossmatch.py wl_id")
        sys.exit(1)
    wl_id = int(sys.argv[1])
# run the crossmatch for this watchlist
    ret = run_watchlist(wl_id)
    hitlist = ret['newhitlist']
    print('Found %d candidates, %d objects' % (ret['ncandidate'], len(hitlist)))
