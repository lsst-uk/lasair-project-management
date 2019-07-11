import os, sys
from astropy.io import fits
from PIL import Image
import numpy
import os.path
import time
import datetime
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common')
import date_nid

def open_fits(filename):
    data = fits.getdata(filename)
    img = Image.fromarray(data)
    y=numpy.asarray(img.getdata(),dtype=numpy.float64).reshape((img.size[1],img.size[0]))

    y = numpy.nan_to_num(y)
    numpy.seterr(invalid='ignore')
#    print(numpy.isnan(y))
    y = numpy.abs(y)
    med = numpy.median(y)
    mymin = 0
    mymax = 3*med
    if mymax-mymin > 1.e-10:
        y = (y-mymin)*255/(mymax-mymin)
        y = numpy.maximum(y, 0)
        y = numpy.minimum(y, 255)
    else:
        y = y * 0.0
    y=numpy.asarray(y,dtype=numpy.uint8)
    return Image.fromarray(y,mode='L')

def convert_fits(dirfits, dirjpg, name):
    t = name.split('_')
    file_ref     = '%s/%s_ref.fits.gz' % (dirfits, t[0])
    file_sci     = '%s/%s_%s_targ_sci.fits.gz' % (dirfits, t[0], t[1])
    file_scimref = '%s/%s_%s_targ_scimref.fits.gz' % (dirfits, t[0], t[1])
    file_refmsci = '%s/%s_%s_targ_refmsci.fits.gz' % (dirfits, t[0], t[1])
    file_jpg     = '%s/%s.jpg' % (dirjpg, t[0])
    if os.path.isfile(file_jpg):
        return 0
    
    border = 5
    size = 63
    new = Image.new('L', (4*border+3*size,2*border+size))
    
    sci = open_fits(file_sci)
    new.paste(sci, (border, border))
    
    ref = open_fits(file_ref)
    new.paste(ref, (size+2*border, border))
    
    try:    diff = open_fits(file_scimref)
    except: diff = open_fits(file_refmsci)
    new.paste(diff, (2*size+3*border,border))
    
    new.save(file_jpg, 'jpeg')
    return 1

def main():
    if len(sys.argv) > 1:
        nid = int(sys.argv[1])
    else:
        nid  = date_nid.nid_now()
    
    date = date_nid.nid_to_date(nid)
    topic  = 'ztf_' + date + '_programid1'

    print('--------------- JPG STAMPS ------------')
    os.system('date')
    print("Topic is %s, nid is %d" % (topic, nid))

    dirfits = '/data/ztf/stamps/fits/%d' % nid
    dirjpg  = '/data/ztf/stamps/jpg/%d'  % nid
    day_total = 0
    run_total = 0
    t = time.time()

    if not os.path.exists(dirjpg):
        print('making jpg directory')
        os.system('mkdir %s' % dirjpg)

    if os.path.exists(dirfits):
        for file in os.listdir(dirfits):
            if file.endswith('_targ_sci.fits.gz'):
                day_total += 1
                run_total += convert_fits(dirfits, dirjpg, file)
    print('------------  JPG STAMPS ----------')
    print('Tried %d stamps, Made %d jpegs' % (day_total, run_total))
    print('Jpegs made in %.1f seconds' % (time.time() - t))

main()
