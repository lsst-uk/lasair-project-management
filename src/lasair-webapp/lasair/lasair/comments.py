from django.shortcuts import render, get_object_or_404, redirect
from django.template.context_processors import csrf
from django.db import connection
from django.contrib.auth.models import User
import lasair.settings
from lasair.models import Comments
import datetime

def new_comment(request):
    if not request.method == 'POST':
        return render(request, 'error.html',{ 'message': "Illegal input no POST"})

    objectId    = request.POST.get('objectId')

    if not request.user.is_authenticated:
        return render(request, 'error.html',{ 'message': "You must be logged in to comment"})

    if 'content' in request.POST:   # submitted comment
        content = request.POST.get('content')
        content = (content[:4000] + '..') if len(content) > 4000 else content

        c = Comments(user=request.user, objectid=objectId, 
            content=content, time=datetime.datetime.now())
        c.save()
        message = "Comment saved successfully"
        return redirect('/object/%s/' % objectId)

    else:   # form for new comment
        return render(request, 'new_comment.html', {'objectId': objectId})

def delete_comment(request, comment_id):
    message = ''
    comment = get_object_or_404(Comments, comment_id=comment_id)
    objectId = comment.objectid

    is_owner = (request.user.is_authenticated) and (request.user == comment.user)
    if not is_owner:
        return render(request, 'error.html',{
            'message': "You are not the owner of this comment"})

    if request.method == 'POST' and is_owner:
        comment.delete()
        return redirect('/object/%s/' % objectId)
