import settings
import random

# setup database connection
import mysql.connector
config = {
    'user'    : settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

# these are the two tables we will work with
# each candidate is a detection of an object, defined by objectId
# each candidate carries a payload that is a random number
table1 = """
create table candidate_toys(
    candid         int NOT NULL AUTO_INCREMENT,
    objectId       varchar(256),
    payload        double,
    PRIMARY KEY (candid)
)
"""

# the object has its textual objectId
# a flag to say that it is stale
# and a collective assessment of all its candidates
# here represented by the maximum value of all the payload attributes
table2 = """
create table object_toys(
    primary_id     int NOT NULL AUTO_INCREMENT,
    objectId       varchar(256),
    stale          tinyint DEFAULT 1,
    payloadMax     double,
    PRIMARY KEY (primary_id),
    UNIQUE KEY objectId_UNIQUE (objectId)
)
"""

def start_again():
# This function drops the two tables and recreates them
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = 'DROP TABLE candidate_toys'
    cursor.execute(query)
    query = 'DROP TABLE object_toys'
    cursor.execute(query)
    cursor.execute(table1)
    cursor.execute(table2)

def print_numbers():
# This function prints numbers of candidates and objects
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT COUNT(*) AS ncand FROM candidate_toys'
    cursor.execute(query)
    for record in cursor: break
    ncand = record['ncand']

    query = 'SELECT COUNT(*) AS nobj FROM object_toys'
    cursor.execute(query)
    for record in cursor: break
    nobj = record['nobj']

    print('  %d candidates and %d objects' % (ncand, nobj))

def make_candidate(n_objects, debug=False):
# thus function creates a new candidate that has a randome payload
# and is attached to a random object
    objectId = 'obj%6x' % random.randrange(n_objects)
    payload = random.random()

# insert the candidate into the table
    query = 'INSERT INTO candidate_toys (objectId, payload) VALUES ("%s", %f)' % (objectId, payload)

# this pair of queries ensures that there is an object created if need be,
# with no exception called if the object already exists.
# If the object exists, the "stale" attribute is set to 1, 
# meaning the payloadMax needs to be recomputed
    query2 = 'INSERT IGNORE INTO object_toys (objectId, stale) VALUES ("%s", 1)' % objectId
    query3 = 'UPDATE object_toys set stale=1 where objectId="%s"' % objectId

    if debug:
        print(query)
        print(query2)
        print(query3)

    cursor = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    cursor.execute(query2)
    cursor.execute(query3)
    msl.commit()

def freshen_object(objectId, candlist, debug=False):
# this object is to be freshened by recomputing the max payload
# given the list of candidates that are associated with the object
    payloadMax = 0.0
    for cand in candlist:
        if cand['payload'] > payloadMax:
            payloadMax = cand['payload']

    query = 'UPDATE object_toys SET payloadMax=%f, stale=0 WHERE objectId="%s"' % (payloadMax, objectId)

    if debug: print(query)
    cursor  = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    msl.commit()

def update_objects(debug=False):
# lots of logic in here, but really it is just to fetch as a single query
# all the objects that need freshening, each with its list of candidates

    cursor  = msl.cursor(buffered=True, dictionary=True)
    n = 0

# if there is a huge number of candidates in the list, it is too big 
# for the memory, so must do it in batches
    nbatch = 500000
    oldObjectId = ''
    candlist = []
    ntotalcand = nupdate = 0
    while(1):
        query =  'SELECT candid, objectId,payload FROM candidate_toys NATURAL JOIN object_toys '
        query += 'WHERE stale=1 ORDER BY objectId LIMIT %d ' % nbatch

        if debug:
            print(query)
        cursor.execute(query)
        for cand in cursor:
            ntotalcand += 1
            objectId = cand['objectId']
            if oldObjectId == '':    # the first record
                oldObjectId = objectId
            if objectId == oldObjectId:  # same again
                candlist.append(cand)
            else:
# once we get a new candid, it is time to freshen the object whose
# list we have built up
                freshen_object(oldObjectId, candlist)
                candlist = [cand]
                oldObjectId = objectId
        if debug:
            print('  iteration: %d candidates, %d updated objects' % (ntotalcand, nupdate))

# count the number of stale objects remaining
        query = 'SELECT COUNT(*) AS nobj FROM object_toys WHERE stale=1'
        cursor.execute(query)
        for record in cursor:
            break
        nobj = record['nobj']
        if nobj == 1:
            result = freshen_object(oldObjectId, candlist)
            nobj -= 1

        if debug and nobj > 0:
            print('  %d objects still stale' % nobj)
        if nobj == 0:
            break

    if debug:
        print('Finished: %d candidates, %d updated objects' % (ntotalcand, nupdate))
