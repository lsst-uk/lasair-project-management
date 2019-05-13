import lasair.settings

def dev(request):
    return {'WEB_DOMAIN': lasair.settings.WEB_DOMAIN}
