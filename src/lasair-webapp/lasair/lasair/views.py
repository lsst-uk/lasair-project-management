from django.shortcuts import render_to_response, get_object_or_404
from django.http import HttpResponseRedirect
from django.template.context_processors import csrf
from lasair.models import Candidates
import lasair.settings
import mysql.connector

def candlist(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        query = request.POST['query'].strip()

        msl = mysql.connector.connect(
            user    =lasair.settings.READONLY_USER,
            password=lasair.settings.READONLY_PASS,
            host    =lasair.settings.DATABASES['default']['HOST'],
            database='ztf')

        cursor = msl.cursor(buffered=True, dictionary=True)
        cursor.execute(query)
        queryset = []
        for row in cursor:
            queryset.append(row)

        return render_to_response('candlist.html',{'table': queryset, 'message': query})
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

    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = ("SELECT * from candidates " + whereClause)
    query = query.replace('htm16ID', 'htmid16')

    prv_cands = []
    cursor.execute(query)
    for row in cursor:
        prv_cands.append(row)
    message = 'hello'

    return render_to_response('cand.html',{'cand': canddict, 'prv_cands': prv_cands, 'message': message})
