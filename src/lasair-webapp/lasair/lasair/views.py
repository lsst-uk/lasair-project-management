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
        cursor.execute('SELECT objectId,ramean,decmean FROM objects WHERE ramean BETWEEN %f and %f AND decmean BETWEEN %f and %f' % (ra-dra, ra+dra, dec-dde, dec+dde))
        hits = cursor.fetchall()
        hitdict = {}
        for hit in hits:
            d = distance(ra, dec, hit[1], hit[2]) * 3600.0
            if d < radius:
                hitdict[hit[0]] = (hit[1], hit[2], d)
        
        return render(request, 'conesearch.html',{'ra':ra, 'dec':dec, 'radius':radius, 'hitdict': hitdict, 'message': message})
    else:
        return render(request, 'conesearch.html',{})

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
