import time
import json
import settings
import queries

candidates = 'candidates'
objects    = 'objects'

# setup database connection
import mysql.connector

def run_query(query, status, msl, file):
    jdnow = (time.time()/86400 + 2440587.5);
    print('jdnow %.3f' % jdnow)
    days_ago_candidates = jdnow - status['cand_max_jd']
    days_ago_objects    = jdnow - status['obj_max_jd']

    sqlquery_real = queries.make_query(query['selected'], query['tables'], query['conditions'], 
        0, 1000, True, days_ago_candidates, days_ago_objects)

    cursor = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(sqlquery_real)
    n = 0
    for record in cursor:
        output = json.dumps(record)
        file.write(output + ',\n')
        n += 1

    return n

def find_queries(status):
    config = {
        'user'    : settings.DB_USER_WRITE,
        'password': settings.DB_PASS_WRITE,
        'host'    : settings.DB_HOST,
        'database': 'ztf'
    }
    msl = mysql.connector.connect(**config)

    cursor   = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT * FROM myqueries2 WHERE active=1'        
    cursor.execute(query)
    for query in cursor:
        print('running query %04d' % query['mq_id'])
        file = open('/data/ztf/streams/substream%04d' % query['mq_id'], 'a')
        n = run_query(query, status, msl, file)
        file.close()
        print('    got %d' % n)

if __name__ == "__main__":
    jsonstr = open('/data/ztf/system_status.json').read()
    status = json.loads(jsonstr)
    find_queries(status)
