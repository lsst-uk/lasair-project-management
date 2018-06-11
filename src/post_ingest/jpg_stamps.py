import os, sys
from astropy.io import fits
from PIL import Image

def open_fits(filename):
    data = fits.getdata(filename)
    img = Image.fromarray(data)
    return img.convert('L')

def convert_fits(dirfits, dirjpg, name):
    t = name.split('_')
    file_ref     = '%s/%s_ref.fits.gz' % (dirfits, t[0])
    file_sci     = '%s/%s_%s_targ_sci.fits.gz' % (dirfits, t[0], t[1])
    file_scimref = '%s/%s_%s_targ_scimref.fits.gz' % (dirfits, t[0], t[1])
    file_refmsci = '%s/%s_%s_targ_refmsci.fits.gz' % (dirfits, t[0], t[1])
    file_jpg     = '%s/%s.jpg' % (dirjpg, t[0])
    
    border = 5
    size = 63
    new = Image.new('L', (4*border+3*size,2*border+size))
    
    ref = open_fits(file_ref)
    new.paste(ref, (border, border))
    
    sci = open_fits(file_sci)
    new.paste(sci, (size+2*border, border))
    
    try:    diff = open_fits(file_scimref)
    except: diff = open_fits(file_refmsci)
    new.paste(diff, (2*size+3*border,border))
    
    new.save(file_jpg, 'jpeg')

def main():
    if len(sys.argv) < 2:
        print 'Usage: python jpg_stamps.py fitsdir jpgdir'
        sys.exit()
    dirfits = sys.argv[1]
    dirjpg  = sys.argv[2]
    for file in os.listdir(dirfits):
        if file.endswith('_targ_sci.fits.gz'):
            convert_fits(dirfits, dirjpg, file)


main()
