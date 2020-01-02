import os, sys
from django.shortcuts import render, get_object_or_404, HttpResponse
from django.template.context_processors import csrf
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q
from django.db import connection
import lasair.settings
from lasair.models import Objects, Comments
from lasair.models import Myqueries
from lasair.models import Watchlists
import mysql.connector
import ephem, math
from datetime import datetime, timedelta
import json
import common.date_nid as date_nid
from common.mag import dc_mag
from common import queries

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def ecliptic(ra, dec):
    np = ephem.Equatorial(math.radians(ra), math.radians(dec), epoch='2000')
    e = ephem.Ecliptic(np)
    return (math.degrees(e.lon), math.degrees(e.lat))

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

        (ec_lon, ec_lat) = ecliptic(objectData['ramean'], objectData['decmean'])
        objectData['ec_lon'] = ec_lon
        objectData['ec_lat'] = ec_lat

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
    query = 'SELECT candid, jd-2400000.5 as mjd, ra, decl, fid, nid, magpsf,sigmapsf, '
    query += 'magnr,sigmagnr, magzpsci, isdiffpos, ssdistnr, ssnamenr, ndethist, '
    query += 'dc_mag, dc_sigmag,dc_mag_g02,dc_mag_g08,dc_mag_g28,dc_mag_r02,dc_mag_r08,dc_mag_r28, '
    query += 'drb '
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

#        d = dc_mag(row['fid'], row['magpsf'],row['sigmapsf'], row['magnr'],row['sigmagnr'], row['magzpsci'], row['isdiffpos'])
#        if row['isdiffpos'] == 'f':
#            row['mag_apparent'] = -2.5*math.log10(10**(-0.4*row['magnr']) - 10**(-0.4*row['magpsf']))
#        else:
#            row['mag_apparent'] = -2.5*math.log10(10**(-0.4*row['magnr']) + 10**(-0.4*row['magpsf']))
#        row['dc_mag'] = d['dc_mag']
#        row['dc_sigmag'] = d['dc_sigmag']

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

def query_list(qs):
    list = []
    if not qs:
        return list
    for q in qs:
        d = {
            'mq_id'      :q.mq_id,
            'usersname'  :q.user.first_name +' '+ q.user.last_name,
            'selected'   :q.selected,
            'tables'     :q.tables,
            'conditions' :q.conditions,
            'name'       :q.name,
            'description':q.description
        }
        d['streamlink'] = 'inactive'
        if q.active:
            topic = queries.topic_name(q.user.id, q.name)
            d['streamlink'] = '<a href="/streamdigest/%s">%s</a>' % (topic, topic)
        list.append(d)
    return list

def streams(request):
    public_queries = Myqueries.objects.filter(public=2)
    return render(request, 'streams.html', {'public_queries':query_list(public_queries)})

def streamdigest(request, topic):
    try:
        data = open('/mnt/lasair-head-data/ztf/streams/%s' % topic, 'r').read()
    except:
        return render(request, 'error.html', {'message': 'Cannot find digest file for ' + topic})
    table = json.loads(data)['digest']
    n = len(table)
    return render(request, 'streamdigest.html', {'topic':topic, 'n':n, 'table':table})

@csrf_exempt
def objlist(request):
    perpage = 1000
    message = ''
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        selected   = request.POST['selected'].strip()
        tables     = request.POST['tables'].strip()
        conditions = request.POST['conditions'].strip()

        json_checked = False
        if 'json' in request.POST and request.POST['json'] == 'on':
            json_checked = True

        check_days_ago = False
        days_ago = 3000
        if 'check_days_ago' in request.POST and request.POST['check_days_ago'] == 'on':
            try:
                days_ago = float(request.POST['days_ago'])
                check_days_ago = True
            except:
                pass

        page     = request.POST['page']
        if len(page.strip()) == 0: page = 0
        else:                      page = int(page)
        ps = page    *perpage
        pe = (page+1)*perpage

        sqlquery_real = queries.make_query(selected, tables, conditions, \
                page, perpage, check_days_ago, days_ago, days_ago)
        message += sqlquery_real

# lets keep a record of all the queries the people try to execute
        record_query(request, sqlquery_real)

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
                    'selected'  :selected, 
                    'tables'    :tables, 
                    'conditions'  :conditions, 
                    'message': message, 'lastpage':lastpage})
    else:
        if request.user.is_authenticated:
            myqueries    = Myqueries.objects.filter(user=request.user)
        else:
            myqueries    = None
        public_queries = Myqueries.objects.filter(public__gte=1)

        if request.user.is_authenticated:
            watchlists = Watchlists.objects.filter(Q(user=request.user) | Q(public__gte=1))
        else:
            watchlists = Watchlists.objects.filter(public__gte=1)

        return render(request, 'objlistquery.html', {
            'is_authenticated': request.user.is_authenticated,
            'myqueries':query_list(myqueries), 
            'watchlists':watchlists,
            'days_ago': 1, 
            'public_queries':query_list(public_queries)})
