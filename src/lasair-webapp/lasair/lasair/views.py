from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponseRedirect
from django.template.context_processors import csrf
from django.db import connection
from django.db.models import Q
from lasair.models import Candidates
import lasair.settings
import mysql.connector
import json
import math

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

def index(request):
    web_domain = lasair.settings.WEB_DOMAIN
    n_candidates = int(open('/mnt/lasair-head-data/ztf/number_candidates.txt').read())
    return render(request, 'index.html', {'web_domain': web_domain, 'n_candidates':n_candidates})

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

def floatsplit(s):
    toks = s.split(':')
    ftoks = []
    for t in toks: 
        ftoks.append(float(t))
    return ftoks

def readcone(cone):
    error = False
    cone = cone.replace(',', '').replace('\t',' ').replace(';',' ').replace('|',' ')
    tok = cone.strip().split()
    
    if len(tok) == 1:
        if tok[0].startswith('ZTF'):
            return {'objectId': tok[0], 'message':'ZTF object'}
        else:
            error = True

    radius = 5.0
    if len(tok) == 3:
        try:
           radius = float(tok[2])
        except:
            error = True

    if len(tok) >= 2:
        try:
            ra  = float(tok[0])
            dec = float(tok[1])
        except:
            try:
                ratok = floatsplit(tok[0])
                detok = floatsplit(tok[1])
                ra = 15*(float(ratok[0] + (ratok[1] + ratok[2]/60)/60))
                if tok[1].startswith('-'):
                    dec = (detok[0] - (detok[1] + detok[2]/60)/60)
                else:
                    dec = (detok[0] + (detok[1] + detok[2]/60)/60)
            except:
                error = True

    if error:
        return {'message': 'cannot parse ' + cone}
    else:
        return {'ra':ra, 'dec':dec, 'radius':radius, 
            'message':'RA,Dec,radius=%.5f,%.5f,%.1f' % (ra, dec, radius)}

def conesearch(request):
    if request.method == 'POST':
        ra = dec = radius = 0.0
        hitdict = {}
        cone = request.POST['cone']
        d = readcone(cone)
        if 'objectId' in d:
            return redirect('/object/%s/' % d['objectId'])
        if 'ra' in d:
            ra = d['ra']
            dec = d['dec']
            radius = d['radius']
            dra = radius/(3600*math.cos(dec*math.pi/180))
            dde = radius/3600
            cursor = connection.cursor()
            query = 'SELECT objectId,ramean,decmean FROM objects WHERE ramean BETWEEN %f and %f AND decmean BETWEEN %f and %f' % (ra-dra, ra+dra, dec-dde, dec+dde)
            cursor.execute(query)
            hits = cursor.fetchall()
            for hit in hits:
                dist = distance(ra, dec, hit[1], hit[2]) * 3600.0
                if dist < radius:
                    hitdict[hit[0]] = (hit[1], hit[2], dist)
            message = d['message'] + '<br/>%d objects found in cone' % len(hitdict)
            return render(request, 'conesearch.html',
                {'ra':ra, 'dec':dec, 'radius':radius, 'cone':cone, 
                    'hitdict': hitdict, 'message': message})
        else:
            return render(request, 'conesearch.html',
                {'cone':cone, 'message': d['message']})
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
#        date1 = '20180528'
        date2 = date_nid.nid_to_date(date_nid.nid_now())
        date1 = date2

    nid1 = date_nid.date_to_nid(date1)
    nid2 = date_nid.date_to_nid(date2)
    return render(request, 'coverage.html',{'nid1':nid1, 'nid2': nid2, 'date1':date1, 'date2':date2})
