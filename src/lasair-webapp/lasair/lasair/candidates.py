from django.shortcuts import render, get_object_or_404
from django.template.context_processors import csrf
from django.db import connection
from lasair.models import Candidates
import lasair.settings
import mysql.connector
import math

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

def candlist(request):
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

# in mysql dec is a reserved word, we used decl in the database
        selected = selected.replace('dec,', 'decl,')
        selectlist = selected.split(',')
# if dec is in the list, change it to decl
        try:
            idx = selectlist.index('dec')
            selectlist[idx] = 'decl'
        except:
            pass
# make sure candid and objectId are in the select list
        if not 'candid' in selectlist:
            selectlist.append('candid')
        if not 'objectId' in selectlist:
            selectlist.append('objectId')
        selected = ','.join(selectlist)

        query = 'SELECT ' + selected + ' FROM candidates'
        if len(where.strip()) > 0:
            query += ' WHERE ' + where.strip()
        if len(order.strip()) > 0:
            query += ' ORDER BY ' + order.strip()
        query += ' LIMIT %d OFFSET %d' % (perpage, page*perpage)
        message = query

        nalert = 0
        msl = connect_db()
        cursor = msl.cursor(buffered=True, dictionary=True)

#        countquery = 'SELECT COUNT(*) AS c FROM candidates'
#        if len(where.strip()) > 0:
#            countquery += ' WHERE ' + where.strip()

#        cursor.execute(countquery)
#        for row in cursor:
#            nalert = int(row['c'])

#        if nalert < pe: 
#            pe = nalert

        cursor.execute(query)
        queryset = []
        for row in cursor:
            queryset.append(row)
            nalert += 1

        return render(request, 'candlist.html',
            {'table': queryset, 'nalert': nalert, 'nextpage': page+1, 'ps':ps, 'pe':pe,  'selected':selected, 'where':where, 'order':order, 'message': message})
    else:
        return render(request, 'candlistquery.html', {})

def cand(request, candid):
    """Show a specific transient"""

    cand = get_object_or_404(Candidates, candid=candid)
    canddict = cand.__dict__
    message = ''

    return render(request, 'cand.html',{'cand': canddict, 'message': message})
