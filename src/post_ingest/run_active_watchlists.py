import time
from common import settings
from common import run_crossmatch

# setup database connection
import mysql.connector

def run_watchlists():
    config = {
        'user'    : settings.DB_USER_WRITE,
        'password': settings.DB_PASS_WRITE,
        'host'    : settings.DB_HOST,
        'database': 'ztf'
    }
    msl = mysql.connector.connect(**config)

    cursor   = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT wl_id, name, public, first_name, last_name '
    query += 'FROM watchlists,auth_user WHERE active=1 AND watchlists.user=auth_user.id'
    cursor.execute(query)
    for row in cursor:
        print('%s %s -- %s:' % (row['first_name'], row['last_name'], row['name']))
        ret = run_crossmatch.run_watchlist(row['wl_id'])
        hitlist = ret['newhitlist']
        print('        %d candidates, %d objects' % (ret['ncandidate'], len(hitlist)))

if __name__ == "__main__":
    print('--------- RUN ACTIVE WATCHLISTS -----------')
    t = time.time()
    run_watchlists()
    print('Active watchlists done in %.1f seconds' % (time.time() - t))
