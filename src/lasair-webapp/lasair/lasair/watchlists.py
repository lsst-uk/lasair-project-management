from django.shortcuts import render, get_object_or_404
from django.template.context_processors import csrf
from django.db import connection
from django.contrib.auth.models import User
from django.http import HttpResponse
import lasair.settings
from lasair.models import Watchlists, WatchlistCones, WatchlistHits
import mysql.connector
import json
from subprocess import Popen, PIPE

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl


def watchlists_home(request):
    message = ''
    if request.method == 'POST' and request.user.is_authenticated:
        delete      = request.POST.get('delete')

        if delete == None:   # create new watchlist
            name        = request.POST.get('name')
            description = request.POST.get('description')
            d_radius    = request.POST.get('radius')
            try:
                default_radius      = float(d_radius)
            except:
                message += 'Cannot parse default radius %s\n' % d_radius

            cone_list = []
            s = request.POST.get('objects')
            for line in s.split('\n'):
                if len(line) == 0: continue
                if line[0] == '#': continue
                line = line.replace('|', ',')
                tok = line.split(',')
                if len(tok) < 2: continue
                try:
                    if len(tok) >= 3:
                        ra       = float(tok[0])
                        dec      = float(tok[1])
                        objectId = tok[2].strip()
                        if len(tok) >= 4: radius = float(tok[3])
                        else:             radius = d_radius
                        cone_list.append([objectId, ra, dec, radius])
                except Exception as e:
                    message += "Bad line %d: %s\n" % (len(cone_list), line)
                    message += str(e)
            if len(cone_list) > 0:
                wl = Watchlists(user=request.user, name=name, description=description, active=0, prequel_where='', radius=default_radius)
                wl.save()
                for cone in cone_list:
                    if len(cone) == 3:
                        wlc = WatchlistCones(wl=wl, name=cone[0], ra=cone[1], decl=cone[2])
                    else:
                        wlc = WatchlistCones(wl=wl, name=cone[0], ra=cone[1], decl=cone[2], radius=cone[3])
                    wlc.save()
                message += '\nWatchlist created successfully with %d sources' % len(cone_list)
        else:
            wl_id = int(delete)
            watchlist = get_object_or_404(Watchlists, wl_id=wl_id)
            if request.user == watchlist.user:
                watchlist.delete()
                message = 'Watchlist %s deleted successfully' % watchlist.name

# public watchlists belong to the anonymous user
    other_watchlists = Watchlists.objects.filter(public=1)
    if request.user.is_authenticated:
        my_watchlists    = Watchlists.objects.filter(user=request.user)
    else:
        my_watchlists    = None

    return render(request, 'watchlists_home.html',
        {'my_watchlists': my_watchlists, 
        'other_watchlists': other_watchlists, 
        'authenticated': request.user.is_authenticated,
        'message': message})

def show_watchlist_txt(request, wl_id):
    message = ''
    watchlist = get_object_or_404(Watchlists, wl_id=wl_id)

    is_owner = (request.user.is_authenticated) and (request.user == watchlist.user)
    is_public = (watchlist.public == 1)
    is_visible = is_owner or is_public
    if not is_visible:
        return render(request, 'error.html',{
            'message': "This watchlist is private and not visible to you"})
    cursor = connection.cursor()
    s = []
    cursor.execute('SELECT ra, decl, name, radius FROM watchlist_cones WHERE wl_id=%d ' % wl_id)
    cones = cursor.fetchall()
    for c in cones:
        if c[3]:
            s += '%f, %f, %s, %f\n' % (c[0], c[1], c[2], c[3])
        else:
            s += '%f, %f, %s\n' % (c[0], c[1], c[2])
    return HttpResponse(s, content_type="text/plain")

def show_watchlist(request, wl_id):
    message = ''
    watchlist = get_object_or_404(Watchlists, wl_id=wl_id)

    is_owner = (request.user.is_authenticated) and (request.user == watchlist.user)
    is_public = (watchlist.public == 1)
    is_visible = is_owner or is_public
    if not is_visible:
        return render(request, 'error.html',{
            'message': "This watchlist is private and not visible to you"})

    if request.method == 'POST' and is_owner:
        if 'name' in request.POST:
            watchlist.name        = request.POST.get('name')
            watchlist.description = request.POST.get('description')

            if request.POST.get('active'): watchlist.active  = 1
            else:                          watchlist.active  = 0

            if request.POST.get('public'): watchlist.public  = 1
            else:                          watchlist.public  = 0

            watchlist.radius      = float(request.POST.get('radius'))
            if watchlist.radius > 360: watchlist.radius = 360
            watchlist.save()
            message += 'watchlist updated'
        else:
            import os
#            from run_crossmatch import run_watchlist
#            hitlist = run_watchlist(wl_id)
            py = lasair.settings.LASAIR_ROOT + 'anaconda3/envs/lasair/bin/python'
            process = Popen([py, lasair.settings.LASAIR_ROOT + 'lasair/src/alert_stream_ztf/common/run_crossmatch.py', '%d'%wl_id], stdout=PIPE, stderr=PIPE)
            stdout, stderr = process.communicate()

            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
            message += 'watchlist crossmatched [%s, %s]' % (stdout, stderr)

    cursor = connection.cursor()
#    cursor.execute('SELECT * FROM watchlist_cones AS c LEFT JOIN watchlist_hits AS h ON c.cone_id = h.cone_id WHERE c.wl_id=%d ORDER BY h.objectId DESC' % wl_id)
    cursor.execute('SELECT * FROM watchlist_cones AS c LEFT JOIN watchlist_hits AS h ON c.cone_id = h.cone_id LEFT JOIN objects on h.objectId = objects.objectId WHERE c.wl_id=%d ' % wl_id)
    cones = cursor.fetchall()
    conelist = []
    found = 0
    for c in cones:
        d = {'name':c[2], 'ra'  :c[3], 'decl' :c[4], 'radius':c[5]}
        if c[8]:
            found += 1
            d['objectId'] = c[8]
            d['arcsec']   = c[9]
            d['sherlock_classification'] = c[32]
            if c[15]:
                d['ncand'] = c[15]
                if c[20]: 
                    grange = c[21] - c[20] 
                else: grange = 0.0
                if c[25]: 
                    rrange = c[25] - c[24] 
                else: rrange = 0.0
                if grange > rrange:
                    d['range'] = grange
                else:
                    d['range'] = rrange
        conelist.append(d)

    def first(d):
        if 'objectId' in d: 
            if 'ncand' in d:
                return '%04d%s' % (d['ncand'], d['objectId'])
            else:
                return '0000%s' % d['objectId']
        else:
            return '000000000'

    conelist.sort(reverse=True, key=first)

    count = len(conelist)
    
    return render(request, 'show_watchlist.html',{
        'watchlist':watchlist, 
        'conelist' :conelist, 
        'count'    :count, 
        'is_owner' :is_owner,
        'message'  :message})

