from django.shortcuts import render_to_response, get_object_or_404
from django.http import HttpResponseRedirect

from lasair.models import Candidates

def candlist(request):
    """Create a text only catalogue of the followup transients"""

    queryset = Candidates.objects.all()
    message = 'hello'
    return render_to_response('candlist.html',{'table': queryset, 'message': message})

