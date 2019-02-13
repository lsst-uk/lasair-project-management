import os
from django.shortcuts import render, get_object_or_404, HttpResponse
from django.template.context_processors import csrf
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
import lasair.settings
from lasair.models import Objects, Myqueries, Comments
import mysql.connector
import ephem, math
from datetime import datetime, timedelta
import json
import date_nid

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def rasex(ra):
    h = math.floor(ra/15)
    ra -= h*15
    m = math.floor(ra*4)
    ra -= m/4.0
    s = ra*240
    return '%02d:%02d:%.3f' % (h, m, s)

def decsex(de):
    ade = abs(de)
    d = math.floor(ade)
    ade -= d
    m = math.floor(ade*60)
    ade -= m/60.0
    s = ade*3600
    if de > 0.0:
        return '%02d:%02d:%.3f' % (d, m, s)
    else:
        return '-%02d:%02d:%.3f' % (d, m, s)

def objhtml(request, objectId):
    data = obj(request, objectId)
    data2 = data.copy()
    if 'comments' in data2:
        data2.pop('comments')
    return render(request, 'show_object.html',
        {'data':data, 'json_data':json.dumps(data2),
        'authenticated': request.user.is_authenticated})

def objjson(request, objectId):
    data = obj(request, objectId)
    if 'comments' in data:
        data.pop('comments')
    return HttpResponse(json.dumps(data), content_type="application/json")

def obj(request, objectId):
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

    comments = []
    if objectData:
        qcomments = Comments.objects.filter(objectid=objectId).order_by('-time')
        for c in qcomments: 
            comments.append(
                {'name':c.user.first_name+' '+c.user.last_name,
                 'content': c.content,
                 'time': c.time,
                 'comment_id': c.comment_id,
                 'mine': (c.user == request.user)})
        message += ' and %d comments' % len(comments)
        message += str(comments)

    crossmatches = []
    if objectData:
        primaryId = int(objectData['primaryId'])
        if objectData and 'annotation' in objectData and objectData['annotation']:
            objectData['annotation'] = objectData['annotation'].replace('"', '').strip()

        objectData['rasex'] = rasex(objectData['ramean'])
        objectData['decsex'] = decsex(objectData['decmean'])

        query = 'SELECT catalogue_object_id, catalogue_table_name, catalogue_object_type, separationArcsec, '
        query += '_r AS r, _g AS g, photoZ, rank '
        query += 'FROM sherlock_crossmatches where transient_object_id = %d ' % primaryId
        query += 'ORDER BY -rank DESC'
        cursor.execute(query)
        for row in cursor:
            if row['rank']:
                crossmatches.append(row)
    message += ' and %d crossmatches' % len(crossmatches)

    candidates = []
    query = 'SELECT candid, jd-2400000.5 as mjd, ra, decl, fid, nid, magpsf, sigmapsf, ssdistnr, ssnamenr, isdiffpos '
    query += 'FROM candidates WHERE objectId = "%s" ' % objectId
    cursor.execute(query)
    count_isdiffpos = count_real_candidates = 0
    for row in cursor:
        mjd = float(row['mjd'])
        date = datetime.strptime("1858/11/17", "%Y/%m/%d")
        date += timedelta(mjd)
        row['utc'] = date.strftime("%Y-%m-%d %H:%M:%S")
        candidates.append(row)
        ssnamenr = row['ssnamenr']
        if ssnamenr == 'null':
            ssnamenr = None
        if row['candid'] and row['isdiffpos'] == 'f':
            count_isdiffpos += 1

    if len(candidates) == 0:
        message = 'objectId %s does not exist'%objectId
        data = {'objectId':objectId, 'message':message}
        return data

    if not objectData:
        ra = float(row['ra'])
        dec = float(row['decl'])
        objectData = {'ramean': ra, 'decmean': dec, 
            'rasex': rasex(ra), 'decsex': decsex(dec),
            'ncand':len(candidates), 'MPCname':ssnamenr}
        objectData['annotation'] = 'Unknown object'
        if row['ssdistnr'] > 0 and row['ssdistnr'] < 10:
            objectData['MPCname'] = ssnamenr

    message += 'Got %d candidates' % len(candidates)

    query = 'SELECT jd-2400000.5 as mjd, fid, diffmaglim '
    query += 'FROM noncandidates WHERE objectId = "%s"' % objectId
    cursor.execute(query)
    for row in cursor:
        mjd = float(row['mjd'])
        date = datetime.strptime("1858/11/17", "%Y/%m/%d")
        date += timedelta(mjd)
        row['utc'] = date.strftime("%Y-%m-%d %H:%M:%S")
        row['magpsf'] = row['diffmaglim']
        candidates.append(row)
    message += 'Got %d candidates and noncandidates' % len(candidates)

    candidates.sort(key= lambda c: c['mjd'], reverse=True)

    data = {'objectId':objectId, 'objectData': objectData, 'candidates': candidates, 
        'count_isdiffpos': count_isdiffpos, 'count_real_candidates':count_real_candidates,
        'crossmatches': crossmatches, 'comments':comments}
    return data

def record_query(request, query):
    onelinequery = query.replace('\r', ' ').replace('\n', ' ')
    time = datetime.now().replace(microsecond=0).isoformat()
    
    if request.user.is_authenticated:
        name = request.user.first_name +' '+ request.user.last_name
    else:
        name = 'anonymous'

    IP       = request.META.get('REMOTE_ADDR')
    if 'HTTP_X_FORWARDED_FOR' in request.META:
        IP = record.request.META['HTTP_X_FORWARDED_FOR']

    date = date_nid.nid_to_date(date_nid.nid_now())
    filename = lasair.settings.QUERY_CACHE + '/' + date
    f = open(filename, 'a')
    s = '%s| %s| %s| %s\n' % (IP, name, time, onelinequery)
    f.write(s)
    f.close()

def streams(request):
    public_queries = Myqueries.objects.filter(public=2)
    return render(request, 'streams.html', {'public_queries':public_queries})

@csrf_exempt
def objlist(request):
    perpage = 1000
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        sqlquery_user = request.POST['sqlquery'].strip()

# lets keep a record of all the queries the people try to execute
        record_query(request, sqlquery_user)

        json_checked = False
        if 'json' in request.POST and request.POST['json'] == 'on':
            json_checked = True

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

        sqlquery_real = 'SELECT /*+ MAX_EXECUTION_TIME(300000) */ ' + ' '.join(tokens[1:])

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

        if json_checked:
            return HttpResponse(json.dumps(queryset), content_type="application/json")
        else:
            return render(request, 'objlist.html',
                {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe, 
                    'sqlquery_user':sqlquery_user, 'message': message, 'lastpage':lastpage})
    else:
        if request.user.is_authenticated:
            myqueries    = Myqueries.objects.filter(user=request.user)
        else:
            myqueries    = None

        public_queries = Myqueries.objects.filter(public__gte=1)
        return render(request, 'objlistquery.html', {
            'is_authenticated': request.user.is_authenticated,
            'myqueries':myqueries, 
            'public_queries':public_queries})
