import sys
import requests
import urllib
import json
import date_nid
import settings
import mysql.connector

if len(sys.argv) > 1:
    nid = int(sys.argv[1])
else:
    print "Usage: check_status.py nid"
    sys.exit()

msl = mysql.connector.connect(\
            user    =settings.DB_USER, \
            password=settings.DB_PASS, \
            host    =settings.DB_HOST, \
            database='ztf')
cursor = msl.cursor(buffered=True, dictionary=True)
query = 'SELECT SUM(n) AS count FROM coverage WHERE nid = %d' % nid
cursor.execute(query)
for row in cursor:
    lasair_count = row['count']
    break


date = date_nid.nid_to_date(nid)

url = 'https://monitor.alerts.ztf.uw.edu/api/datasources/proxy/7/api/v1/query?query='

urltail = 'sum(kafka_log_log_value{ name="LogEndOffset" , night = "%s", program = "MSIP" }) - sum(kafka_log_log_value{ name="LogStartOffset", night = "%s", program="MSIP" })' % (date, date)

urlquote = url + urllib.quote(urltail)

resultjson = requests.get(urlquote, auth=('ztf', 'fullofstars'))
result = json.loads(resultjson.text)
alertsstr = result['data']['result'][0]['value'][1]
alerts = int(alertsstr)
print "Processed: %d of %d" % (lasair_count, alerts)

