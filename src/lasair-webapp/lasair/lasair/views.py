from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponseRedirect, HttpResponse
from django.template.context_processors import csrf
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
from django.db.models import Q
from lasair.models import Candidates, Myqueries
import lasair.settings
import mysql.connector
import json
import math
import time
import common.date_nid as date_nid

from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate

import string
import random
def id_generator(size=10):
   chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
   return ''.join(random.choice(chars) for _ in range(size))

def signup(request):
    if request.method == 'POST':
        first_name    = request.POST['first_name']
        last_name     = request.POST['last_name']
        username      = request.POST['username']
        email         = request.POST['email']
        password      = id_generator(10)
        user = User.objects.create_user(username=username, first_name=first_name,last_name=last_name, email=email, password=password)
        user.save()
        return redirect('/accounts/password_reset/')
    else:
        return render(request, 'signup.html')

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def status(request):
    message = ''
    web_domain = lasair.settings.WEB_DOMAIN
    jsonstr = open('/mnt/lasair-head-data/ztf/system_status.json').read()
    status = json.loads(jsonstr)
    unix_time = time.time()
    time_since_update = time.time() - status['update_time_unix']
    if time_since_update > 3600:
        message = 'No update in %d seconds! ' % int(time_since_update)
    z = status['today_candidates_ztf']
    l = status['today_candidates_lasair']
    if z > 2000 and z-l > 60000:
        message += 'ZTF reports %d candidates today, only %d ingested by Lasair!' % (z, l)
    status['minutes_since'] = int(time_since_update/60.0)
    return render(request, 'status.html', {'web_domain': web_domain, 'status':status, 'message':message})

def index(request):
    web_domain = lasair.settings.WEB_DOMAIN
    public_queries = Myqueries.objects.filter(public=2)
    return render(request, 'index.html', {'web_domain': web_domain, 'public_queries':public_queries})

def about(request):
    jsonstr = open('/mnt/lasair-head-data/ztf/system_status.json').read()
    j = json.loads(jsonstr)
    n_candidates = j['total_candidates']
    return render(request, 'about.html', {'n_candidates':n_candidates})

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

def sexra(tok):
    return 15*(float(tok[0]) + (float(tok[1]) + float(tok[2])/60)/60)

def sexde(tok):
    if tok[0].startswith('-'):
        de = (float(tok[0]) - (float(tok[1]) + float(tok[2])/60)/60)
    else:
        de = (float(tok[0]) + (float(tok[1]) + float(tok[2])/60)/60)
    return de

def readcone(cone):
    error = False
    message = ''
    cone = cone.replace(',', ' ').replace('\t',' ').replace(';',' ').replace('|',' ')
    tok = cone.strip().split()
#    message += str(tok)

# if tokens begin with 'ZTF', must be list of ZTF identifier
    allztf = True
    for t in tok:
        if not t.startswith('ZTF'):
            allztf = False
    if allztf:
        return {'objectIds': tok, 'message':'ZTF object list'}

# if odd number of tokens, must end with radius in arcsec
    radius = 5.0
    if len(tok)%2 == 1:
        try:
           radius = float(tok[-1])
        except:
            error = True
        tok = tok[:-1]

# remaining options tok=2 and tok=6
#   radegrees decdegrees
#   h:m:s   d:m:s
#   h m s   d m s
    if len(tok) == 2:
        try:
            ra = float(tok[0])
            de = float(tok[1])
        except:
            try:
                ra = sexra(tok[0].split(':'))
                de = sexde(tok[1].split(':'))
            except:
                error = True

    if len(tok) == 6:
        try:
            ra = sexra(tok[0:3])
            de = sexde(tok[3:6])
        except:
            error = True

    if error:
        return {'message': 'cannot parse ' + cone + ' ' + message}
    else:
        message += 'RA,Dec,radius=%.5f,%.5f,%.1f' % (ra, de, radius)
        return {'ra':ra, 'dec':de, 'radius':radius, 'message':message}

def conesearch(request):
    if request.method == 'POST':
        cone = request.POST['cone']
        json_checked = False
        if 'json' in request.POST and request.POST['json'] == 'on':
            json_checked = True

        data = conesearch_impl(cone)
        if json_checked:
            return HttpResponse(json.dumps(data), content_type="application/json")
        else:
            return render(request, 'conesearch.html', {'data':data})
    else:
        return render(request, 'conesearch.html', {})

def conesearch_impl(cone):
    ra = dec = radius = 0.0
#    hitdict = {}
    hitlist = []
    d = readcone(cone)
    if 'objectIds' in d:
        data = {'cone':cone, 'hitlist': d['objectIds'], 
            'message': 'Found ZTF object names'}
        return data
    if 'ra' in d:
        ra = d['ra']
        dec = d['dec']
        radius = d['radius']
        dra = radius/(3600*math.cos(dec*math.pi/180))
        dde = radius/3600
        cursor = connection.cursor()
#            query = 'SELECT objectId,ramean,decmean FROM objects WHERE ramean BETWEEN %f and %f AND decmean BETWEEN %f and %f' % (ra-dra, ra+dra, dec-dde, dec+dde)
        query = 'SELECT DISTINCT objectId FROM candidates WHERE ra BETWEEN %f and %f AND decl BETWEEN %f and %f' % (ra-dra, ra+dra, dec-dde, dec+dde)
        cursor.execute(query)
        hits = cursor.fetchall()
        for hit in hits:
#            dist = distance(ra, dec, hit[1], hit[2]) * 3600.0
#            if dist < radius:
#                hitdict[hit[0]] = (hit[1], hit[2], dist)
             hitlist.append(hit[0])
        message = d['message'] + '<br/>%d objects found in cone' % len(hitlist)
        data = {'ra':ra, 'dec':dec, 'radius':radius, 'cone':cone,
                'hitlist': hitlist, 'message': message}
        return data
    else:
        data = {'cone':cone, 'message': d['message']}
        return data

def coverage(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        date1 = request.POST['date1'].strip()
        date2 = request.POST['date2'].strip()
        if date1 == 'today': date1 = date_nid.nid_to_date(date_nid.nid_now())
        if date2 == 'today': date2 = date_nid.nid_to_date(date_nid.nid_now())
    else:
#        date1 = '20180528'
        date2 = date_nid.nid_to_date(date_nid.nid_now())
        date1 = '20180528'

    nid1 = date_nid.date_to_nid(date1)
    nid2 = date_nid.date_to_nid(date2)
    return render(request, 'coverage.html',{'nid1':nid1, 'nid2': nid2, 'date1':date1, 'date2':date2})
