from django.shortcuts import render, get_object_or_404, redirect
from django.template.context_processors import csrf
from django.db import connection
from django.contrib.auth.models import User
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import mysql.connector
import lasair.settings
from lasair.models import Comments
import datetime

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READWRITE_USER,
        password=lasair.settings.READWRITE_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl

@csrf_exempt
def new_comment(request):
    if not request.method == 'POST':
        return render(request, 'error.html',{ 'message': "Illegal input no POST"})

    key = request.POST.get('citizenScienceKey')
    if key != lasair.settings.CITIZEN_SCIENCE_KEY:
        return render(request, 'error.html',{ 'message': "Wrong key"})

    objectId    = request.POST.get('objectId')
    if objectId == 'deleteall':
        query = 'DELETE FROM comments WHERE user=%d' % lasair.settings.CITIZEN_SCIENCE_USERID
    else:
        content = request.POST.get('content')
        content = (content[:4000] + '..') if len(content) > 4000 else content
        query = 'INSERT INTO comments (user, objectId, content) '
        query += 'VALUES (%d, "%s", "%s")' % (lasair.settings.CITIZEN_SCIENCE_USERID, objectId, content)

    msl = connect_db()
    cursor = msl.cursor(buffered=True, dictionary=True)
    cursor.execute(query)
    msl.commit()
    return HttpResponse('success')
