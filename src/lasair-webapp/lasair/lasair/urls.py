"""lasair URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.contrib.auth import views as authviews
from django.urls import include, path
from django.views.generic import TemplateView

from lasair import views, services, candidates, objects, watchlists, myqueries, comments, cs_comments, skymap
import lasair.settings

from django.contrib import admin
#admin.autodiscover()

urlpatterns = [
    path('',                        views.index,                name='index'),
    path('about',                   views.about,                name='about'),
    path('sherlock',  TemplateView.as_view(template_name='sherlock.html')),
    path('lasairlsst',  TemplateView.as_view(template_name='lasairlsst.html')),
    path('cand/<int:candid>/',      candidates.cand,            name='cand'),

    path('objlist/',                objects.objlist,            name='objlist'),
    path('streams/',                objects.streams,            name='streams'),
    path('streamdigest/<slug:topic>/',   objects.streamdigest,           name='streamdigest'),
    path('object/<slug:objectId>/',      objects.objhtml,                name='objhtml'),
    path('object/<slug:objectId>/json/', objects.objjson,                name='objjson'),

    path('conesearch/',             views.conesearch,           name='conesearch'),

    path('watchlist/',              watchlists.watchlists_home, name='watchlists_home'),
    path('watchlist/<int:wl_id>/',  watchlists.show_watchlist,  name='show_watchlist'),
    path('watchlist/<int:wl_id>/txt/',  watchlists.show_watchlist_txt,  name='show_watchlist_txt'),

    path('myquery/',                myqueries.new_myquery,      name='new_myquery'),
    path('myquery/<int:mq_id>/',    myqueries.show_myquery,     name='show_myquery'),

    path('comment/',                comments.new_comment,       name='new_comment'),
    path('delete_comment/<int:comment_id>/',    comments.delete_comment,    name='delete_comment'),
    path('cs_comment/',             cs_comments.new_comment,    name='new_comment'),

    path('coverage/',               views.coverage,             name='coverage'),
    path('status/',                 views.status,               name='status'),

    path('skymap/',                 skymap.skymap,              name='skymap'),
    path('skymap/<skymap_id_version>/',     skymap.show_skymap,         name='show_skymap'),

    path('schema',   TemplateView.as_view(template_name='schema.html')),
    path('jupyter',  TemplateView.as_view(template_name='jupyter.html')),
    path('release',  TemplateView.as_view(template_name='release.html')),
    path('contact',  TemplateView.as_view(template_name='contact.html')),
    path('coverageAjax/<int:nid1>/<int:nid2>/',\
                                     services.coverageAjax,     name='coverageAjax'),
    path('accounts/', include('django.contrib.auth.urls')),
    path('signup/',                  views.signup,              name='signup'),
    path('admin/',                   admin.site.urls),
]
