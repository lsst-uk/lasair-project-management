from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponseRedirect, HttpResponse
from django.template.context_processors import csrf
from django.db import connection
import lasair.settings
import mysql.connector
import json
import math
import time
import date_nid

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

from subprocess import Popen, PIPE
def skymap(request):
    message = ''
    p = Popen(['ls', '/mnt/lasair-head-data/ztf/skymap/'], stdout=PIPE)
    skymap_list = []
    result = p.communicate()[0].decode("utf-8")
    for file in result.split('\n'):
        tok = file.split('.')
        if len(tok) == 2 and tok[1] == 'json':
            skymap_list.append(tok[0])
    return render(request, 'skymap.html', {'skymap_list': skymap_list, 'message': message})

import dateutil.parser as dp
def jd_from_iso(date):
    if not date.endswith('Z'): 
        date += 'Z'
    parsed_t = dp.parse(date)
    unix = int(parsed_t.strftime('%s'))
    jd = unix/86400 + 2440587.5
    return jd

def show_skymap(request, skymap_id):
    json_text = open("/mnt/lasair-head-data/ztf/skymap/%s.json" % skymap_id).read()
    skymap_data = json.loads(json_text)
    isodate = skymap_data['meta']['DATE-OBS']
    niddate1 = niddate2 = isodate.split('T')[0].replace('-', '')
    tilelist = skymap_data['meta']['histogram']
    jd1 = jd2 = jd = jd_from_iso(isodate)
    ztf_wanted = coverage_wanted = False

    if request.method == 'POST':
        ztf_wanted = (request.POST.get('ztf_wanted','off') == 'on')
        jd1 = float(request.POST['jd1'])
        jd2 = float(request.POST['jd2'])
        coverage_wanted = (request.POST.get('coverage_wanted','off')  == 'on')
        niddate1 = request.POST['niddate1']
        niddate2 = request.POST['niddate2']

    nid1 = date_nid.date_to_nid(niddate1)
    nid2 = date_nid.date_to_nid(niddate2)

    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)

# ZTF candidates
    ztf_data = []
    if ztf_wanted:
        query = "SELECT objectId,jd,ra,decl,fid,magpsf "
        query += "FROM candidates WHERE jd BETWEEN %f and %f AND (" % (jd1, jd2)
        constraint_list = []
        for tile in tilelist:
            constraint = "(ra BETWEEN %.1f and %.1f AND decl BETWEEN %.1f AND %.1f)"
            constraint = constraint % (5*tile[0], 5*tile[0]+5, 90-5*tile[1]-5, 90-5*tile[1])
            constraint_list.append(constraint)
        query += " OR ".join(constraint_list) + ") ORDER BY objectId"
    
        cursor.execute(query)
        for row in cursor:
            ztf_data.append({\
                'objectId'   :row['objectId'], \
                'jd'   :row['jd'], \
                'ra'   :row['ra'], \
                'dec'  :row['decl'],
                'fid'  :row['fid'],
                'magpsf'  :row['magpsf'],
                })

# Coverage
    coverage = []
    if coverage_wanted:
        query = "SELECT field,fid,ra,decl,SUM(n) as sum "
        query += "FROM coverage WHERE nid BETWEEN %d and %d GROUP BY field,fid,ra,decl" % (nid1, nid2)

        cursor.execute(query)
        for row in cursor:
            coverage.append({\
                'field':row['field'], \
                'fid'  :row['fid'], \
                'ra'   :row['ra'], \
                'dec'  :row['decl'],
                'n'    :int(row['sum'])})

    return render(request, 'show_skymap.html', 
        {'skymap_id': skymap_id, 'isodate':isodate, 
            'niddate1':niddate1, 'niddate2':niddate2, 
            'jd':jd, 'jd1':jd1, 'jd2':jd2,
            'coverage_wanted': coverage_wanted,
            'coverage': json.dumps(coverage),
            'ztf_wanted': ztf_wanted,
            'nztf': len(ztf_data), 'ztf_data':json.dumps(ztf_data)})
