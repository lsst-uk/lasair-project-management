import sys
import requests
import urllib
import json
import time
import mysql.connector
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import settings
import date_nid
import datetime

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
cursor.execute(query)
for row in cursor:
    total_candidates = row['total_candidates']
    break

query = 'SELECT SUM(n) AS count FROM coverage WHERE nid = %d' % nid
cursor.execute(query)
for row in cursor:
    today_candidates_lasair = row['count']
    break
if not today_candidates_lasair:
    today_candidates_lasair = 0

date = date_nid.nid_to_date(nid)
url = 'https://monitor.alerts.ztf.uw.edu/api/datasources/proxy/7/api/v1/query?query='
urltail = 'sum(kafka_log_log_value{ name="LogEndOffset" , night = "%s", program = "MSIP" }) - sum(kafka_log_log_value{ name="LogStartOffset", night = "%s", program="MSIP" })' % (date, date)

urlquote = url + urllib.quote(urltail)
resultjson = requests.get(urlquote, 
    auth=(settings.GRAFANA_USERNAME, settings.GRAFANA_PASSWORD))
result = json.loads(resultjson.text)
alertsstr = result['data']['result'][0]['value'][1]
today_candidates_ztf = int(alertsstr)

update_time = datetime.datetime.now().isoformat()
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
}

dictstr = json.dumps(dict)


print('------------- SYSTEM STATUS -------------')
print(dictstr)

f = open('/data/ztf/system_status.json', 'w')
f.write(dictstr)
f.close()
