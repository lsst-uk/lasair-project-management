from django.db import models

class Candidates(models.Model):
    objectid = models.CharField(db_column='objectId', max_length=16, blank=True, null=True)  # Field name made lowercase.
    jd = models.FloatField(blank=True, null=True)
    fid = models.IntegerField(blank=True, null=True)
    pid = models.BigIntegerField(blank=True, null=True)
    diffmaglim = models.FloatField(blank=True, null=True)
    pdiffimfilename = models.CharField(max_length=255, blank=True, null=True)
    programpi = models.CharField(max_length=255, blank=True, null=True)
    programid = models.IntegerField(blank=True, null=True)
    candid = models.BigIntegerField(primary_key=True)
    isdiffpos = models.CharField(max_length=255, blank=True, null=True)
    tblid = models.BigIntegerField(blank=True, null=True)
    nid = models.IntegerField(blank=True, null=True)
    rcid = models.IntegerField(blank=True, null=True)
    field = models.IntegerField(blank=True, null=True)
    xpos = models.FloatField(blank=True, null=True)
    ypos = models.FloatField(blank=True, null=True)
    ra = models.FloatField(blank=True, null=True)
    decl = models.FloatField(blank=True, null=True)
    magpsf = models.FloatField(blank=True, null=True)
    sigmapsf = models.FloatField(blank=True, null=True)
    chipsf = models.FloatField(blank=True, null=True)
    magap = models.FloatField(blank=True, null=True)
    sigmagap = models.FloatField(blank=True, null=True)
    distnr = models.FloatField(blank=True, null=True)
    magnr = models.FloatField(blank=True, null=True)
    sigmagnr = models.FloatField(blank=True, null=True)
    chinr = models.FloatField(blank=True, null=True)
    sharpnr = models.FloatField(blank=True, null=True)
    sky = models.FloatField(blank=True, null=True)
    magdiff = models.FloatField(blank=True, null=True)
    fwhm = models.FloatField(blank=True, null=True)
    classtar = models.FloatField(blank=True, null=True)
    mindtoedge = models.FloatField(blank=True, null=True)
    magfromlim = models.FloatField(blank=True, null=True)
    seeratio = models.FloatField(blank=True, null=True)
    aimage = models.FloatField(blank=True, null=True)
    bimage = models.FloatField(blank=True, null=True)
    aimagerat = models.FloatField(blank=True, null=True)
    bimagerat = models.FloatField(blank=True, null=True)
    elong = models.FloatField(blank=True, null=True)
    nneg = models.IntegerField(blank=True, null=True)
    nbad = models.IntegerField(blank=True, null=True)
    rb = models.FloatField(blank=True, null=True)
    ssdistnr = models.FloatField(blank=True, null=True)
    ssmagnr = models.FloatField(blank=True, null=True)
    ssnamenr = models.CharField(max_length=255, blank=True, null=True)
    sumrat = models.FloatField(blank=True, null=True)
    magapbig = models.FloatField(blank=True, null=True)
    sigmagapbig = models.FloatField(blank=True, null=True)
    ranr = models.FloatField(blank=True, null=True)
    decnr = models.FloatField(blank=True, null=True)
    sgmag1 = models.FloatField(blank=True, null=True)
    srmag1 = models.FloatField(blank=True, null=True)
    simag1 = models.FloatField(blank=True, null=True)
    szmag1 = models.FloatField(blank=True, null=True)
    sgscore1 = models.FloatField(blank=True, null=True)
    distpsnr1 = models.FloatField(blank=True, null=True)
    ndethist = models.IntegerField(blank=True, null=True)
    ncovhist = models.IntegerField(blank=True, null=True)
    jdstarthist = models.FloatField(blank=True, null=True)
    jdendhist = models.FloatField(blank=True, null=True)
    scorr = models.FloatField(blank=True, null=True)
    tooflag = models.IntegerField(blank=True, null=True)
    objectidps1 = models.BigIntegerField(blank=True, null=True)
    objectidps2 = models.BigIntegerField(blank=True, null=True)
    sgmag2 = models.FloatField(blank=True, null=True)
    srmag2 = models.FloatField(blank=True, null=True)
    simag2 = models.FloatField(blank=True, null=True)
    szmag2 = models.FloatField(blank=True, null=True)
    sgscore2 = models.FloatField(blank=True, null=True)
    distpsnr2 = models.FloatField(blank=True, null=True)
    objectidps3 = models.BigIntegerField(blank=True, null=True)
    sgmag3 = models.FloatField(blank=True, null=True)
    srmag3 = models.FloatField(blank=True, null=True)
    simag3 = models.FloatField(blank=True, null=True)
    szmag3 = models.FloatField(blank=True, null=True)
    sgscore3 = models.FloatField(blank=True, null=True)
    distpsnr3 = models.FloatField(blank=True, null=True)
    nmtchps = models.IntegerField(blank=True, null=True)
    rfid = models.BigIntegerField(blank=True, null=True)
    jdstartref = models.FloatField(blank=True, null=True)
    jdendref = models.FloatField(blank=True, null=True)
    nframesref = models.IntegerField(blank=True, null=True)
    htmid16 = models.BigIntegerField(blank=True, null=True)
    rbversion = models.CharField(max_length=16, blank=True, null=True)
    dsnrms = models.FloatField(blank=True, null=True)
    ssnrms = models.FloatField(blank=True, null=True)
    dsdiff = models.FloatField(blank=True, null=True)
    magzpsci = models.FloatField(blank=True, null=True)
    magzpsciunc = models.FloatField(blank=True, null=True)
    magzpscirms = models.FloatField(blank=True, null=True)
    nmatches = models.IntegerField(blank=True, null=True)
    clrcoeff = models.FloatField(blank=True, null=True)
    clrcounc = models.FloatField(blank=True, null=True)
    zpclrcov = models.FloatField(blank=True, null=True)
    zpmed = models.FloatField(blank=True, null=True)
    clrmed = models.FloatField(blank=True, null=True)
    clrrms = models.FloatField(blank=True, null=True)
    neargaia = models.FloatField(blank=True, null=True)
    neargaiabright = models.FloatField(blank=True, null=True)
    maggaia = models.FloatField(blank=True, null=True)
    maggaiabright = models.FloatField(blank=True, null=True)
    exptime = models.FloatField(blank=True, null=True)
    drb = models.FloatField(blank=True, null=True)
    drbversion = models.CharField(max_length=16, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'candidates'
        app_label = 'Candidates'

#class Noncandidates(models.Model):
#    primaryid = models.AutoField(db_column='primaryId', primary_key=True)  # Field name made lowercase.
#    objectid = models.CharField(db_column='objectId', max_length=16, blank=True, null=True)  # Field name made lowercase.
#    jd = models.FloatField(blank=True, null=True)
#    fid = models.IntegerField(blank=True, null=True)
#    diffmaglim = models.FloatField(blank=True, null=True)
#
#    class Meta:
#        managed = False
#        db_table = 'noncandidates'

class Objects(models.Model):
    primaryid = models.AutoField(db_column='primaryId', primary_key=True)  # Field name made lowercase.
    objectid = models.CharField(db_column='objectId', unique=True, max_length=16, blank=True, null=True)  # Field name made lowercase.
    ncand = models.IntegerField()
    ramean = models.FloatField()
    rastd = models.FloatField()
    decmean = models.FloatField()
    decstd = models.FloatField()
    maggmin = models.FloatField()
    maggmax = models.FloatField()
    maggmedian = models.FloatField()
    maggmean = models.FloatField()
    magrmin = models.FloatField()
    magrmax = models.FloatField()
    magrmedian = models.FloatField()
    magrmean = models.FloatField()
    latestmag = models.FloatField()
    jdmin = models.FloatField()
    jdmax = models.FloatField()

    class Meta:
        managed = False
        db_table = 'objects'


# A watchlist is owned by a user and given a name and description
# Only active watchlists are run against the realtime ingestion
# The prequel_where can be used to select which candidates are compared with the watchlist
from django.contrib.auth.models import User

# When a watchlist is run against the database, ZTF candidates may be matched to cones
# We also keep the objectId of that candidate and distance from the cone center
# If the same run happens again, that candidate will not go in again to the same watchlist.

class WatchlistCones(models.Model):
    cone_id = models.AutoField(primary_key=True)
    wl      = models.ForeignKey('Watchlists', models.DO_NOTHING, blank=True, null=True)
    name    = models.CharField(max_length=32, blank=True, null=True)
    ra      = models.FloatField(blank=True, null=True)
    decl    = models.FloatField(blank=True, null=True)
    radius  = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'watchlist_cones'

class WatchlistHits(models.Model):
    candid   = models.BigIntegerField(primary_key=True)
    wl       = models.ForeignKey('Watchlists', models.DO_NOTHING)
    cone     = models.ForeignKey(WatchlistCones, models.DO_NOTHING, blank=True, null=True)
    objectid = models.CharField(db_column='objectId', max_length=16, blank=True, null=True)  # Field name made lowercase.
    arcsec   = models.FloatField(blank=True, null=True)
    name     = models.CharField(max_length=32, blank=True, null=True)

    class Meta:
        managed         = False
        db_table        = 'watchlist_hits'
        unique_together = (('candid', 'wl'),)

class Watchlists(models.Model):
    wl_id         = models.AutoField(primary_key=True)
    user          = models.ForeignKey(User, models.DO_NOTHING, db_column='user', blank=True, null=True)
    name          = models.CharField(max_length=256, blank=True, null=True)
    description   = models.CharField(max_length=4096, blank=True, null=True)
    active        = models.IntegerField(blank=True, null=True)
    public        = models.IntegerField(blank=True, null=True)
    prequel_where = models.CharField(max_length=4096, blank=True, null=True)
    radius        = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'watchlists'

#class Myqueries(models.Model):
    #mq_id = models.AutoField(primary_key=True)
    #user = models.ForeignKey(User, models.DO_NOTHING, db_column='user', blank=True, null=True)
    #name = models.CharField(max_length=256, blank=True, null=True)
    #description = models.CharField(max_length=4096, blank=True, null=True)
    #query = models.CharField(max_length=4096, blank=True, null=True)
    #public = models.IntegerField(blank=True, null=True)
#
    #class Meta:
        #managed = False
        #db_table = 'myqueries'

class Myqueries(models.Model):
    mq_id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User, models.DO_NOTHING, db_column='user', blank=True, null=True)
    name = models.CharField(max_length=256, blank=True, null=True)
    description = models.CharField(max_length=4096, blank=True, null=True)
    selected = models.CharField(max_length=4096, blank=True, null=True)
    conditions = models.CharField(max_length=4096, blank=True, null=True)
    tables = models.CharField(max_length=4096, blank=True, null=True)
    public = models.IntegerField(blank=True, null=True)
    active = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'myqueries2'


class Comments(models.Model):
    comment_id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User, models.DO_NOTHING, db_column='user', blank=True, null=True)
    objectid = models.CharField(db_column='objectId', unique=True, max_length=16, blank=True, null=True)  # Field name made lowercase.
    content = models.CharField(max_length=4096, blank=True, null=True)
    time = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'comments'

