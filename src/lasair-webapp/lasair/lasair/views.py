from django.shortcuts import render, get_object_or_404
from django.http import HttpResponseRedirect
from django.template.context_processors import csrf
from django.db import connection
from django.db.models import Q
from lasair.models import Candidates
import lasair.settings
import mysql.connector
import json
import math

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
    return render(request, 'index.html', {'web_domain': web_domain, 'n_candidates':n_candidates})

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

        return render(request, 'candlist.html',
            {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe,  'selected':selected, 'where':where, 'order':order, 'message': message})
    else:
        return render(request, 'candlistquery.html', {})

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

    return render(request, 'cand.html',{'cand': canddict, 'prv_cands': prv_cands, 'message': message})

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

def conesearch(request):
    message = ''
    if request.method == 'POST':
        ra     = float(request.POST['ra'])
        dec    = float(request.POST['dec'])
        radius = float(request.POST['radius'])
        dra = radius/(3600*math.cos(dec*math.pi/180))
        dde = radius/3600
        cursor = connection.cursor()
        cursor.execute('SELECT objectId,ra,decl FROM candidates WHERE ra BETWEEN %f and %f AND decl BETWEEN %f and %f' % (ra-dra, ra+dra, dec-dde, dec+dde))
        hits = cursor.fetchall()
        hitdict = {}
        for hit in hits:
            d = distance(ra, dec, hit[1], hit[2]) * 3600.0
            if d < radius:
                hitdict[hit[0]] = (hit[1], hit[2], d)
        
        return render(request, 'conesearch.html',{'ra':ra, 'dec':dec, 'radius':radius, 'hitdict': hitdict, 'message': message})
    else:
        return render(request, 'conesearch.html',{})

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
    return render(request, 'show_object.html',{'objectId':objectId, 'cands': cands,'json_cands':json_data, 'message': message})

import date_nid
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
    return render(request, 'coverage.html',{'nid1':nid1, 'nid2': nid2, 'date1':date1, 'date2':date2})


from lasair.models import Watchlists, WatchlistCones, WatchlistHits

def watchlists_home(request):
    message = ''
    if request.method == 'POST' and request.user.is_authenticated:
        delete      = request.POST.get('delete')
        if delete == None:
            name        = request.POST.get('name')
            description = request.POST.get('description')
            d_radius    = request.POST.get('radius')
            try:
                default_radius      = float(d_radius)
            except:
                message += 'Cannot parse default radius %s\n' % d_radius

            cone_list = []
            for line in request.POST.get('objects').split('\n'):
                tok = line.split(',')
                try:
                    if len(tok) >= 3:
                        objectId = tok[0].strip()
                        ra       = float(tok[1])
                        dec      = float(tok[2])
                        cone_list.append([objectId, ra, dec])
                except:
                    message += "Bad line %d: %s\n" % (len(cone_list), line)
            if len(message) == 0:
                wl = Watchlists(user=request.user, name=name, description=description, active=0, prequel_where='', radius=default_radius)
                wl.save()
                for cone in cone_list:
                    wlc = WatchlistCones(wl=wl, name=cone[0], ra=cone[1], decl=cone[2])
                    wlc.save()
                message = 'Watchlist created successfully'
        else:
            wl_id = int(delete)
            watchlist = get_object_or_404(Watchlists, wl_id=wl_id)
            if request.user == watchlist.user:
                watchlist.delete()
                message = 'Watchlist %s deleted successfully' % watchlist.name

    if request.user.is_authenticated:
        my_watchlists    = Watchlists.objects.filter(user=request.user)
        other_watchlists = Watchlists.objects.filter(~Q(user=request.user))
    else:
        my_watchlists    = None
        other_watchlists = Watchlists.objects.all()

    return render(request, 'watchlists_home.html',
        {'my_watchlists': my_watchlists, 
        'other_watchlists': other_watchlists, 
        'authenticated': request.user.is_authenticated,
        'message': message})

def show_watchlist(request, wl_id):
    message = ''
    watchlist = get_object_or_404(Watchlists, wl_id=wl_id)

    is_owner = request.user.is_authenticated and request.user == watchlist.user

    if request.method == 'POST' and is_owner:
        if 'name' in request.POST:
            watchlist.name        = request.POST.get('name')
            watchlist.description = request.POST.get('description')
            if request.POST.get('active'):
                watchlist.active  = 1
            else:
                watchlist.active  = 0
            watchlist.radius      = request.POST.get('radius')
            watchlist.save()
            message += 'watchlist updated'
        else:
            import os
#            from run_crossmatch import run_watchlist
#            hitlist = run_watchlist(wl_id)
            cmd = '/home/roy/anaconda3/envs/lasair/bin/python /home/roy/lasair-dev/src/alert_stream_ztf/common/run_crossmatch.py %d' % wl_id
            os.system(cmd)
            message += 'watchlist crossmatched'

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM watchlist_cones AS c LEFT JOIN watchlist_hits AS h ON c.cone_id = h.cone_id WHERE c.wl_id=%d ORDER BY h.objectId DESC' % wl_id)
    cones = cursor.fetchall()
    conelist = []
    for c in cones:
        d = {'name':c[2], 'ra'  :c[3], 'decl' :c[4]}
        if c[6]:
            d['objectId'] = c[7]
            d['arcsec']   = c[8]
            d['ndethist'] = c[9]
        conelist.append(d)
    count = len(conelist)
    
    return render(request, 'show_watchlist.html',{
        'watchlist':watchlist, 
        'conelist' :conelist, 
        'count'    :count, 
        'is_owner' :is_owner,
        'message'  :message})

