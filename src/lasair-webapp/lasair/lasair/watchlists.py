from django.shortcuts import render, get_object_or_404
from django.template.context_processors import csrf
from django.db import connection
from django.contrib.auth.models import User
import lasair.settings
from lasair.models import Watchlists, WatchlistCones, WatchlistHits
import mysql.connector
import json

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
#    cursor.execute('SELECT * FROM watchlist_cones AS c LEFT JOIN watchlist_hits AS h ON c.cone_id = h.cone_id WHERE c.wl_id=%d ORDER BY h.objectId DESC' % wl_id)
    cursor.execute('SELECT * FROM watchlist_cones AS c LEFT JOIN watchlist_hits AS h ON c.cone_id = h.cone_id LEFT JOIN objects on h.objectId = objects.objectId WHERE c.wl_id=%d ' % wl_id)
    cones = cursor.fetchall()
    conelist = []
    for c in cones:
        d = {'name':c[2], 'ra'  :c[3], 'decl' :c[4]}
        if c[7]:
            d['objectId'] = c[7]
            d['arcsec']   = c[8]
            d['sherlock_classification'] = c[30]
            if c[13]:
                d['ncand'] = c[13]
                if c[19]: 
                    grange = c[19] - c[18] 
                else: grange = 0.0
                if c[23]: 
                    rrange = c[23] - c[22] 
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

