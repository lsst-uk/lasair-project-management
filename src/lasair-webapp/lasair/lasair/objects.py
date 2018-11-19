from django.shortcuts import render, get_object_or_404, HttpResponse
from django.template.context_processors import csrf
from django.db import connection
import lasair.settings
from lasair.models import Objects, Myqueries
import mysql.connector
import ephem, math
from datetime import datetime, timedelta
import json

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def objhtml(request, objectId):
    data = obj(objectId)
    return render(request, 'show_object.html',
        {'data':data, 'json_data':json.dumps(data)})

def objjson(request, objectId):
    data = obj(objectId)
    return HttpResponse(json.dumps(data), content_type="application/json")

def obj(objectId):
    """Show a specific object, with all its candidates"""
    objectData = None
    message = ''
    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT o.primaryId, o.ncand, o.ramean, o.decmean, o.glonmean, o.glatmean, s.classification, s.annotation, s.separationArcsec  '
    query += 'FROM objects AS o LEFT JOIN sherlock_classifications AS s ON o.primaryId = s.transient_object_id '
    query += 'WHERE stale != 1 AND o.objectId = "%s"' % objectId
    cursor.execute(query)
    for row in cursor:
        objectData = row

    crossmatches = []
    if objectData:
        message += str(objectData)
        primaryId = int(objectData['primaryId'])
        if objectData and 'annotation' in objectData and objectData['annotation']:
            objectData['annotation'] = objectData['annotation'].replace('"', ' arcsec').strip()

        query = 'SELECT catalogue_object_id, catalogue_table_name, catalogue_object_type, separationArcsec, '
        query += '_r AS r, _g AS g, photoZ, rank '
        query += 'FROM sherlock_crossmatches where transient_object_id = %d ' % primaryId
        query += 'ORDER BY -rank DESC'
        cursor.execute(query)
        for row in cursor:
            crossmatches.append(row)
    message += ' and %d crossmatches' % len(crossmatches)

    candidates = []
    query = 'SELECT candid, jd, ra, decl, fid, nid, magpsf, sigmapsf, ssdistnr, ssnamenr '
    query += 'FROM candidates WHERE objectId = "%s" ' % objectId
    cursor.execute(query)
    for row in cursor:
        jd = float(row['jd'])
        mjd = jd - 2400000.5
        date = datetime.strptime("1858/11/17", "%Y/%m/%d")
        date += timedelta(mjd)
        row['utc'] = date.strftime("%Y-%m-%d %H:%M:%S")
        candidates.append(row)

    if not objectData:
        objectData = {'ramean': row['ra'], 'decmean': row['decl'], 
            'ncand':len(candidates), 'MPCname':row['ssnamenr']}
        objectData['annotation'] = 'Unknown object'
        if row['ssdistnr'] < 10:
            objectData['MPCname'] = row['ssnamenr']

    message += 'Got %d candidates' % len(candidates)

    query = 'SELECT jd, fid, diffmaglim '
    query += 'FROM noncandidates WHERE objectId = "%s"' % objectId
    cursor.execute(query)
    for row in cursor:
        jd = float(row['jd'])
        mjd = jd - 2400000.5
        date = datetime.strptime("1858/11/17", "%Y/%m/%d")
        date += timedelta(mjd)
        row['utc'] = date.strftime("%Y-%m-%d %H:%M:%S")
        row['magpsf'] = row['diffmaglim']
        candidates.append(row)
    message += 'Got %d candidates and noncandidates' % len(candidates)

    candidates.sort(key= lambda c: c['jd'], reverse=True)


    data = {'objectId':objectId, 'objectData': objectData, 'candidates': candidates, 'crossmatches': crossmatches}
    return data


def objlist(request):
    perpage = 1000
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        sqlquery_user = request.POST['sqlquery'].strip()

        page     = request.POST['page']
        if len(page.strip()) == 0: page = 0
        else:                      page = int(page)
        ps = page    *perpage
        pe = (page+1)*perpage

        tokens = sqlquery_user.split()
        firstword = tokens[0].lower()
        if firstword != 'select':
            message = 'You must start the query with the word "SELECT"'
            return render(request, 'error.html', {'message': message})

        sqlquery_real = 'SELECT /*+ MAX_EXECUTION_TIME(60000) */ ' + ' '.join(tokens[1:])

        sqlquery_real += ' LIMIT %d OFFSET %d' % (perpage, page*perpage)
        message = sqlquery_real

        nalert = 0
        msl = connect_db()
        cursor = msl.cursor(buffered=True, dictionary=True)

        try:
            cursor.execute(sqlquery_real)
        except Exception as e:
            message = 'Your query:<br/><b>' + sqlquery_real + '</b><br/>returned the error<br/><i>' + str(e) + '</i>'
            return render(request, 'error.html', {'message': message})

        queryset = []
        for row in cursor:
            queryset.append(row)
            nalert += 1
        lastpage = 0
        if ps + nalert < pe:
            pe = ps + nalert
            lastpage = 1

        return render(request, 'objlist.html',
            {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe,  'sqlquery_user':sqlquery_user, 'message': message, 'lastpage':lastpage})

    else:
        if request.user.is_authenticated:
            myqueries    = Myqueries.objects.filter(user=request.user)
        else:
            myqueries    = None

        public_queries = Myqueries.objects.filter(public=1)
        return render(request, 'objlistquery.html', {
            'is_authenticated': request.user.is_authenticated,
            'myqueries':myqueries, 
            'public_queries':public_queries})
