import settings
import mysql.connector
msl = mysql.connector.connect(\
            user    =settings.DB_USER, \
            password=settings.DB_PASS, \
            host    =settings.DB_HOST, \
            database='ztf')
cursor = msl.cursor(buffered=True, dictionary=True)
query = 'SELECT count(*) AS n FROM candidates'
cursor.execute(query)
for row in cursor:
    n = row['n']
    break
f = open('/data/ztf/number_candidates.txt', 'w')
print>>f, n
