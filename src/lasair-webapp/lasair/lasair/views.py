from django.shortcuts import render_to_response, get_object_or_404
from django.http import HttpResponseRedirect

from lasair.models import Candidates

def candlist(request):
    """Create a text only catalogue of the followup transients"""

    queryset = Candidates.objects.all()
    message = 'hello'
    return render_to_response('candlist.html',{'table': queryset, 'message': message})

def cand(request, candid):
    """Show a specific transient"""

    cand = get_object_or_404(Candidates, candid=candid)


    import sys
    sys.path.append('/home/roy/ps1/code/utils/htm_utils/python/swig')
    import htmCircle
    import mysql.connector


    radius_arcsec = 30.0
    whereClause = htmCircle.htmCircleRegion(16, cand.ra, cand.decl, radius_arcsec)

    msl = mysql.connector.connect(user='ztf', password='OPV537', host='lasair-db', database='ztf')
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = ("SELECT * from candidates " + whereClause)
    query = query.replace('htm16ID', 'htmid16')

    prv_cands = []
    cursor.execute(query)
    for row in cursor:
        prv_cands.append(row)
    message = 'hello'

    return render_to_response('cand.html',{'cand': cand, 'prv_cands': prv_cands, 'message': message})
