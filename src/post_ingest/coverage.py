import sys
from common import settings
import time

print('---------- COVERAGE TABLE ------------')
import mysql.connector
msl = mysql.connector.connect(\
            user    =settings.DB_USER_WRITE, \
            password=settings.DB_PASS_WRITE, \
            host    =settings.DB_HOST, \
            database='ztf')

cursor  = msl.cursor(buffered=True, dictionary=True)


t = time.time()
query = 'DROP TABLE IF EXISTS coverage2'
cursor.execute(query)

query = 'CREATE TABLE coverage2 AS '
query += '(SELECT candidates.field, fid, nid, fields.ra, fields.decl, COUNT(*) AS n '
query += 'FROM candidates INNER JOIN fields ON candidates.field=fields.field '
query += 'WHERE candidates.ndethist > 0 '
query += 'GROUP BY candidates.field, fid, nid)'
cursor.execute(query)

query = 'DROP TABLE IF EXISTS coverage'
cursor.execute(query)

query = 'RENAME TABLE coverage2 TO coverage'
cursor.execute(query)

print('Coverage table rebuilt in %.1f seconds' % (time.time() - t))
msl.commit()
