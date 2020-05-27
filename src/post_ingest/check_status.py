import sys
import requests
import urllib
import urllib.parse
import json
import time
import mysql.connector
from common import settings
from common import date_nid
import datetime

print('------------- CHECK STATUS -------------')
t = time.time()
if len(sys.argv) > 1:
    nid = int(sys.argv[1])
else:
    print ("Usage: check_status.py nid")
    sys.exit()

msl = mysql.connector.connect(\
            user    =settings.DB_USER, \
            password=settings.DB_PASS, \
            host    =settings.DB_HOST, \
            database='ztf')

cursor = msl.cursor(buffered=True, dictionary=True)
query = 'SELECT SUM(n) AS total_candidates FROM coverage'

try:
    cursor.execute(query)
    for row in cursor:
        total_candidates = row['total_candidates']
        break
except:
    sys.exit()

query = 'SELECT count(*) AS count FROM candidates WHERE nid = %d' % nid
cursor.execute(query)
for row in cursor:
    today_candidates_lasair = row['count']
    break
if not today_candidates_lasair:
    today_candidates_lasair = 0

query = 'SELECT MAX(jd) AS cand_max_jd FROM candidates'
cursor.execute(query)
for row in cursor:
    cand_max_jd = row['cand_max_jd']
    break

query = 'SELECT MAX(jdmax) AS obj_max_jd FROM objects'
cursor.execute(query)
for row in cursor:
    obj_max_jd = row['obj_max_jd']
    break

date = date_nid.nid_to_date(nid)
url = 'https://monitor.alerts.ztf.uw.edu/api/datasources/proxy/7/api/v1/query?query='
urltail = 'sum(kafka_log_log_value{ name="LogEndOffset" , night = "%s", program = "MSIP" }) - sum(kafka_log_log_value{ name="LogStartOffset", night = "%s", program="MSIP" })' % (date, date)

try:
    urlquote = url + urllib.parse.quote(urltail)
    resultjson = requests.get(urlquote, 
        auth=(settings.GRAFANA_USERNAME, settings.GRAFANA_PASSWORD))
    result = json.loads(resultjson.text)
    alertsstr = result['data']['result'][0]['value'][1]
    today_candidates_ztf = int(alertsstr)
except:
     today_candidates_ztf = -1

update_time = datetime.datetime.utcnow().isoformat()
update_time = update_time.split('.')[0]
update_time_unix = time.time()

dict = {
    'total_candidates'       : int(total_candidates), 
    'today_candidates_lasair': int(today_candidates_lasair),
    'today_candidates_ztf'   : today_candidates_ztf,
    'update_time'            : update_time,
    'update_time_unix'       : update_time_unix,
    'nid'                    : nid,
    'date'                   : date,
    'cand_max_jd'            : cand_max_jd,
    'obj_max_jd'             : obj_max_jd,
}

dictstr = json.dumps(dict)


print(dictstr)

f = open('/data/ztf/system_status.json', 'w')
f.write(dictstr)
f.close()

print('Check status finished in %.1f seconds' % (time.time() - t))
