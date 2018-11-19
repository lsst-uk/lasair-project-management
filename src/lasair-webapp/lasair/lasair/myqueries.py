from django.shortcuts import render, get_object_or_404
from django.template.context_processors import csrf
from django.db import connection
from django.contrib.auth.models import User
import lasair.settings
from lasair.models import Myqueries

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl


#def myqueries_home(request):
#    message = ''
#    if request.method == 'POST' and request.user.is_authenticated:
#        delete      = request.POST.get('delete')
#
#        if delete == None:   # create new myquery
#            name        = request.POST.get('name')
#            description = request.POST.get('description')
#            query       = request.POST.get('query')
#            if request.POST.get('public') == 'on':
#                public = 1
#            else:
#                public = 0
#
#            mq = Myqueries(user=request.user, name=name, description=description, 
#                public=public, query=query)
#            mq.save()
#            message = 'Query created successfully'
#        else:
#            mq_id = int(delete)
#            myquery = get_object_or_404(Myqueries, mq_id=mq_id)
#            if request.user == myquery.user:
#                myquery.delete()
#                message = 'Query %s deleted successfully' % myquery.name

# public myqueries belong to the anonymous user
#    other_queries = Myqueries.objects.filter(public=1)
#    if request.user.is_authenticated:
#        myqueries    = Myqueries.objects.filter(user=request.user)
#    else:
#        myqueries    = None
#
#    return render(request, 'myqueries_home.html',
#        {'myqueries': myqueries, 
#        'other_queries': other_queries, 
#        'authenticated': request.user.is_authenticated,
#        'message': message})

def new_myquery(request):
    is_owner = (request.user.is_authenticated)
    return render(request, 'new_myquery.html',{
        'is_owner' :is_owner,
    })

def show_myquery(request, mq_id):
    message = ''
    myquery = get_object_or_404(Myqueries, mq_id=mq_id)

    is_owner = (request.user.is_authenticated) and (request.user == myquery.user)
    is_public = (myquery.public == 1)
    is_visible = is_owner or is_public
    if not is_visible:
        return render(request, 'error.html',{
            'message': "This query is private and not visible to you"})

    if request.method == 'POST' and is_owner:
        myquery.name        = request.POST.get('name')
        myquery.description = request.POST.get('description')
        myquery.query       = request.POST.get('query')
        myquery.public      = request.POST.get('public')

        if request.POST.get('public'): myquery.public  = 1
        else:                          myquery.public  = 0

        myquery.save()
        message += 'query updated'

    return render(request, 'show_myquery.html',{
        'myquery'  :myquery, 
        'is_owner' :is_owner,
        'message'  :message})

