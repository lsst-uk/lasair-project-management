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
from lasair import views, services

from django.contrib import admin
#admin.autodiscover()

urlpatterns = [
    path('',                        views.index,    name='index'),
    path('candlist/',               views.candlist, name='candlist'),
    path('cand/<int:candid>/',      views.cand,     name='cand'),
    path('object/<slug:objectId>/', views.show_object,   name='show_object'),
    path('conesearch/',             views.conesearch, name='conesearch'),
    path('watchlist/',              views.watchlists_home,     name='watchlists_home'),
    path('watchlist/<int:wl_id>/',  views.show_watchlist,     name='show_watchlist'),
    path('coverage/',               views.coverage, name='coverage'),
    path('jupyter',  TemplateView.as_view(template_name='jupyter.html')),
    path('release',  TemplateView.as_view(template_name='release.html')),
    path('contact',  TemplateView.as_view(template_name='contact.html')),
    path('coverageAjax/<int:nid1>/<int:nid2>/',\
                                     services.coverageAjax, name='coverageAjax'),
    path('accounts/', include('django.contrib.auth.urls')),
    path('admin/', admin.site.urls),
]
