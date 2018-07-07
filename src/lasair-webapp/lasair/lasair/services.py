from django.shortcuts import render_to_response, get_object_or_404
from django.http import HttpResponseRedirect
from django.template.context_processors import csrf
from lasair.models import Candidates
import lasair.settings
import mysql.connector
from django.http import JsonResponse

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def coverageAjax(request, nid1, nid2):
    """Show a specific transient"""

    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)
    if nid1 <= nid2:   # date range
        dict = {'queryType': 'dateRange', 'nid1':nid1, 'nid2':nid2}
        query = "SELECT candidates.field,fid,fields.ra,fields.decl,COUNT(*) AS n "
        query += "FROM candidates INNER JOIN fields ON candidates.field=fields.field "
        query += "WHERE nid BETWEEN %d AND %d  " % (nid1, nid2+1)
        query += "GROUP BY candidates.field,fid"

    else:              # all dates
        dict = {'queryType': 'allDates'}
        query = "SELECT candidates.field,fid,fields.ra,fields.decl,COUNT(*) AS n "
        query += "FROM candidates INNER JOIN fields ON candidates.field=fields.field "
        query += "GROUP BY candidates.field,fid"

    cursor.execute(query)
    result = []
    for row in cursor:
        result.append({\
            'field':row['field'], \
            'fid'  :row['fid'], \
            'ra'   :row['ra'], \
            'dec'  :row['decl'], 
            'n'    :row['n']})

    dict['result'] = result
    return JsonResponse(dict)
