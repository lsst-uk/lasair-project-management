from django.shortcuts import render_to_response, get_object_or_404
from django.http import HttpResponseRedirect
from django.template.context_processors import csrf
from lasair.models import Candidates
import lasair.settings
import mysql.connector
import json

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def index(request):
    web_domain = lasair.settings.WEB_DOMAIN
    n_candidates = int(open('/mnt/lasair-head-data/ztf/number_candidates.txt').read())
    return render_to_response('index.html', {'web_domain': web_domain, 'n_candidates':n_candidates})

def candlist(request):
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

# in mysql dec is a reserved word, we used decl in the database
        selected = selected.replace('dec,', 'decl,')
        selectlist = selected.split(',')
# if dec is in the list, change it to decl
        try:
            idx = selectlist.index('dec')
            selectlist[idx] = 'decl'
        except:
            pass
# make sure candid and objectId are in the select list
        if not 'candid' in selectlist:
            selectlist.append('candid')
        if not 'objectId' in selectlist:
            selectlist.append('objectId')
        selected = ','.join(selectlist)

        query = 'SELECT ' + selected + ' FROM candidates'
        if len(where.strip()) > 0:
            query += ' WHERE ' + where.strip()
        if len(order.strip()) > 0:
            query += ' ORDER BY ' + order.strip()
        query += ' LIMIT %d OFFSET %d' % (perpage, page*perpage)
        message = query

        countquery = 'SELECT COUNT(*) AS c FROM candidates'
        if len(where.strip()) > 0:
            countquery += ' WHERE ' + where.strip()

        msl = connect_db()
        cursor = msl.cursor(buffered=True, dictionary=True)
        cursor.execute(countquery)
        nalert = 0
        for row in cursor:
            nalert = int(row['c'])

        if nalert < pe: 
            pe = nalert

        cursor.execute(query)
        queryset = []
        for row in cursor:
            queryset.append(row)

        return render_to_response('candlist.html',
            {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe,  'selected':selected, 'where':where, 'order':order, 'message': message})
    else:
        return render_to_response('candlistquery.html', {})

def cand(request, candid):
    """Show a specific transient"""

    cand = get_object_or_404(Candidates, candid=candid)
    canddict = cand.__dict__


    import sys
    sys.path.append('/home/roy/ps1/code/utils/htm_utils/python/swig')
    import htmCircle


    radius_arcsec = 30.0
    whereClause = htmCircle.htmCircleRegion(16, cand.ra, cand.decl, radius_arcsec)

    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = ("SELECT * from candidates " + whereClause)
    query = query.replace('htm16ID', 'htmid16')

    prv_cands = []
    cursor.execute(query)
    for row in cursor:
        if row['candid'] != candid:
            prv_cands.append(row)
    message = 'hello'

    return render_to_response('cand.html',{'cand': canddict, 'prv_cands': prv_cands, 'message': message})

def show_object(request, objectId):
    """Show a specific object, with all its candidates"""
    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = ('SELECT * from candidates WHERE objectId = "' + objectId + '" ORDER BY jd')
    cands = []
    cursor.execute(query)
    for row in cursor:
        cands.append(row)
    message = 'Got %d candidates' % len(cands)
    json_data = json.dumps(cands)
    return render_to_response('show_object.html',{'objectId':objectId, 'cands': cands,'json_cands':json_data, 'message': message})

from lasair import date_nid
def coverage(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        date1 = request.POST['date1'].strip()
        date2 = request.POST['date2'].strip()
        if date1 == 'today': date1 = date_nid.nid_to_date(date_nid.nid_now())
        if date2 == 'today': date2 = date_nid.nid_to_date(date_nid.nid_now())
    else:
        date1 = '20180528'
        date2 = date_nid.nid_to_date(date_nid.nid_now())

    nid1 = date_nid.date_to_nid(date1)
    nid2 = date_nid.date_to_nid(date2)
    return render_to_response('coverage.html',{'nid1':nid1, 'nid2': nid2, 'date1':date1, 'date2':date2})
