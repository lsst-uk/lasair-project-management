from django.shortcuts import render, get_object_or_404, redirect
from django.template.context_processors import csrf
from django.db import connection
from django.db.models import Q
from django.contrib.auth.models import User
import lasair.settings
from lasair.models import Myqueries
from lasair.models import Watchlists
from common import queries

def connect_db():
    msl = mysql.connector.connect(
        user    =lasair.settings.READONLY_USER,
        password=lasair.settings.READONLY_PASS,
        host    =lasair.settings.DATABASES['default']['HOST'],
        database='ztf')
    return msl


def new_myquery(request):
    is_owner = (request.user.is_authenticated)
    message = ''
    email = ''
    if request.user.is_authenticated:
        email = request.user.email
        watchlists = Watchlists.objects.filter(Q(user=request.user) | Q(public__gte=1))
    else:
        watchlists = Watchlists.objects.filter(public__gte=1)

    if request.method == 'POST' and is_owner:
        name        = request.POST.get('name')
        description = request.POST.get('description')
        selected       = request.POST.get('selected')
        conditions     = request.POST.get('conditions')
        tables         = request.POST.get('tables')
        try:
            active      = int(request.POST.get('active'))
        except:
            active = 0
        public      = request.POST.get('public')

        mq = Myqueries(user=request.user, name=name, description=description, 
                public=public, active=active, selected=selected, conditions=conditions, tables=tables)
        mq.save()
        message = "Query saved successfully"
        return render(request, 'show_myquery.html',{
            'myquery' :mq, 
            'watchlists': watchlists,
            'is_owner' :is_owner,
            'message'  :message})

    return render(request, 'new_myquery.html',{
        'watchlists': watchlists,
        'email': email,
        'is_owner' :is_owner
    })

def show_myquery(request, mq_id):
    message = ''
    email = ''
    myquery = get_object_or_404(Myqueries, mq_id=mq_id)

    is_owner = (request.user.is_authenticated) and (request.user == myquery.user)
    is_public = (myquery.public == 1)
    is_active = (myquery.active > 0)
    is_visible = is_owner or is_active
    if not is_visible:
        return render(request, 'error.html',{
            'message': "This query is private and not visible to you"})

    if request.method == 'POST' and is_owner:
        if 'delete' in request.POST:
            myquery.delete()
            return redirect('/objlist/')
        else:
            myquery.name        = request.POST.get('name')
            myquery.description = request.POST.get('description')
            myquery.selected     = request.POST.get('selected')
            myquery.tables       = request.POST.get('tables')
            myquery.conditions   = request.POST.get('conditions')
            public            = request.POST.get('public')
            myquery.active       = int(request.POST.get('active'))

            if public: 
                if myquery.public == 0: 
                    myquery.public  = 1 # if set to 1 or 2 leave it as it is
            else:
                myquery.public  = 0

            myquery.save()
            message += str(myquery.active) + 'query updated'

    if request.user.is_authenticated:
        email = request.user.email
        watchlists = Watchlists.objects.filter(Q(user=request.user) | Q(public__gte=1))
    else:
        watchlists = Watchlists.objects.filter(public__gte=1)

    return render(request, 'show_myquery.html',{
        'myquery' :myquery, 
        'email' : email,
        'watchlists':watchlists,
        'topic'   : queries.topic_name(myquery.user.id, myquery.name),
        'is_owner' :is_owner,
        'message'  :message})
