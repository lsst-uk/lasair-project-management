from django.shortcuts import render, get_object_or_404, HttpResponse
from django.template.context_processors import csrf
from django.db import connection
import lasair.settings
from lasair.models import Objects
import mysql.connector
from astropy.time import Time
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
    message = ''
    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT o.primaryId, o.ncand, o.ramean, o.decmean, s.classification, s.annotation, s.separationArcsec  '
    query += 'FROM objects AS o LEFT JOIN sherlock_classifications AS s ON o.primaryId = s.transient_object_id '
    query += 'WHERE o.objectId = "%s"' % objectId
    cursor.execute(query)
    for row in cursor:
        objectData = row
    message += str(objectData)
    primaryId = int(objectData['primaryId'])
    if objectData and 'annotation' in objectData and objectData['annotation']:
        objectData['annotation'] = objectData['annotation'].replace('"', ' arcsec')

    query = 'SELECT candid, jd, ra, decl, fid, nid, magpsf, sigmapsf, distpsnr1, sgscore1, sgmag1, srmag1 '
    query += 'FROM candidates WHERE objectId = "%s" ORDER BY jd DESC ' % objectId
    candidates = []
    cursor.execute(query)
    for row in cursor:
        jd = float(row['jd'])
        t = Time(jd, format='jd')
        row['utc'] = t.iso
        candidates.append(row)
    message += 'Got %d candidates' % len(candidates)

    query = 'SELECT catalogue_object_id, catalogue_table_id, catalogue_object_type, separationArcsec, '
    query += '_r AS r, _g AS g, photoZ, rank '
    query += 'FROM sherlock_crossmatches where transient_object_id = %d ' % primaryId
    query += 'ORDER BY -rank DESC'
    crossmatches = []
    cursor.execute(query)
    for row in cursor:
        crossmatches.append(row)
    message += ' and %d crossmatches' % len(crossmatches)

    data = {'objectId':objectId, 'objectData': objectData, 'candidates': candidates, 'crossmatches': crossmatches}
    return data


def objlist(request):
    perpage = 100
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        selected = request.POST['selected'].strip()
        where    = request.POST['where'].strip()
        order    = request.POST['order'].strip()

        page     = request.POST['page']
        if len(page.strip()) == 0: page = 0
        else:                      page = int(page)
        ps = page    *perpage
        pe = (page+1)*perpage

        selectlist = selected.split(',')
        if not 'objectId' in selectlist:
            selectlist.insert(0, 'objectId')
        selected = ','.join(selectlist)

        query = 'SELECT ' + selected + ' FROM objects'
        if len(where.strip()) > 0:
            query += ' WHERE ' + where.strip()
        if len(order.strip()) > 0:
            query += ' ORDER BY ' + order.strip()
        query += ' LIMIT %d OFFSET %d' % (perpage, page*perpage)
        message = query

        nalert = 0
        msl = connect_db()
        cursor = msl.cursor(buffered=True, dictionary=True)

        cursor.execute(query)
        queryset = []
        for row in cursor:
            queryset.append(row)
            nalert += 1

        return render(request, 'objlist.html',
            {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe,  'selected':selected, 'where':where, 'order':order, 'message': message})
    else:
        return render(request, 'objlistquery.html', {})
