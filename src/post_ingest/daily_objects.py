import sys
import settings

import mysql.connector
msl = mysql.connector.connect(\
            user    =settings.DB_USER_WRITE, \
            password=settings.DB_PASS_WRITE, \
            host    =settings.DB_HOST, \
            database='ztf')
cursor = msl.cursor(buffered=True, dictionary=True)
query = 'DROP TABLE objects'
cursor.execute(query)
print ('table dropped')
query = 'CREATE TABLE objects AS SELECT objectId, candid FROM candidates WHERE (objectId, jd) IN (SELECT objectId, MIN(jd) FROM candidates GROUP BY objectId)'
cursor.execute(query)
print ('table created')
