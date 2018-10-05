from django.shortcuts import render, get_object_or_404
from django.template.context_processors import csrf
from django.db import connection
import lasair.settings
from lasair.models import Objects
import mysql.connector
import json

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def obj(request, objectId):
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

def objlist(request):
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

        selectlist = selected.split(',')
        if not 'objectId' in selectlist:
            selectlist.insert(0, 'objectId')
        selected = ','.join(selectlist)

        query = 'SELECT ' + selected + ' FROM objects'
        if len(where.strip()) > 0:
            query += ' WHERE ' + where.strip()
        if len(order.strip()) > 0:
            query += ' ORDER BY ' + order.strip()
        query += ' LIMIT %d OFFSET %d' % (perpage, page*perpage)
        message = query

        nalert = 0
        msl = connect_db()
        cursor = msl.cursor(buffered=True, dictionary=True)

        cursor.execute(query)
        queryset = []
        for row in cursor:
            queryset.append(row)
            nalert += 1

        return render(request, 'objlist.html',
            {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe,  'selected':selected, 'where':where, 'order':order, 'message': message})
    else:
        return render(request, 'objlistquery.html', {})
