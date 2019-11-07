import time
import json
from common import settings
from common import queries
from confluent_kafka import Producer, KafkaError

candidates = 'candidates'
objects    = 'objects'

# setup database connection
import mysql.connector

def run_query(query, status, msl, topic):
    jdnow = (time.time()/86400 + 2440587.5);
#    print('jdnow %.3f' % jdnow)
    days_ago_candidates = jdnow - status['cand_max_jd']
    days_ago_objects    = jdnow - status['obj_max_jd']

    sqlquery_real = queries.make_query(query['selected'], query['tables'], query['conditions'], 
        0, 1000, True, days_ago_candidates, days_ago_objects)

    cursor = msl.cursor(buffered=True, dictionary=True)
    n = 0
    try:
        cursor.execute(sqlquery_real)
        file = open('/data/ztf/streams/%s' % topic, 'a')
        for record in cursor:
            output = json.dumps(record)
            file.write(output + ',\n')
            n += 1
        file.close()
    except:
        print("Query failed for %s" % topic)
        print(sqlquery_real)

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
        topic = queries.topic_name(query['name'])
        n = run_query(query, status, msl, topic)
        print('query %s got %d' % (topic, n))

if __name__ == "__main__":
    print('--------- RUN ACTIVE QUERIES -----------')
    t = time.time()
    jsonstr = open('/data/ztf/system_status.json').read()
    status = json.loads(jsonstr)
    find_queries(status)
    print('Active queries done in %.1f seconds' % (time.time() - t))
