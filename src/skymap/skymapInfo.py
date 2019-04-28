import sys
import math
import json
import numpy
import healpy
from colorsys import hsv_to_rgb
import matplotlib
matplotlib.use('Agg') # switch to agg to suppress plotting windows 
import matplotlib.pyplot as plt

import galaxyCatalog
glade_filename = '/data/ztf/skymap/galaxy_catalog/glade.fits'

radian = 180/numpy.pi

def get_md(file):
    tok = file.split('/')
    filename = tok[-1]
    skymapID = filename.split('.')[0]
    return skymapID.lower()

def splitPaths(path):
    paths = []
    d = 0.0
    first = path[0]
    q = first
    newpath = []
    for p in path:
        d += abs(p[0]-q[0]) + abs(p[1]-q[1])
        q = p
        if d > 30:
            newpath.append([p[0],p[1]])
            paths.append(newpath)
            newpath = []
            d = 0.0
        newpath.append([p[0],p[1]])
    paths.append(newpath)
    return paths

def makeFile(infile, outfile, language='json'):
    print("From ", infile, " to ", outfile)
    s = generate_from_fitsfile(infile, language)

    f= open(outfile, 'w')
    f.write(s)
    f.close()
    return 1

def generate_from_fitsfile(filename, language):
    healpix_data,header = healpy.read_map(filename, h=True)
    try:
        healpix_data, distmu, distsigma, distnorm = healpy.read_map(filename, field=[0, 1, 2, 3])
        dim = 3
    except:
        dim = 2

    print("dimension = %d" % dim)
    if dim == 3:
        ii = (~numpy.isinf(distmu)) & (distmu > 0.0)
        print("good percent %.2f" % (numpy.sum(ii)*100.0/len(distmu)))
        dm = distmu[ii]
        prob = healpix_data[ii]
        x = numpy.multiply(prob,dm)
        print("Average distance %.2f " % (numpy.sum(x)/numpy.sum(prob)))

    wanted = ['DATE', 'DATE-OBS', 'INSTRUME', 'MJD-OBS', 'DISTMEAN', 'DISTSTD', 'REFERENC']
    meta = {}
    for (k,v) in header:
        if k in wanted:
            meta[k] = v

    print("Generating contours...")
    skymapID = get_md(filename)

    contours = []
    levels = []
    deciles = []
    percentile = numpy.linspace(0.9, 0.1, 9)
    for p in percentile:
        (level, area) = get_level(healpix_data, p)
        levels.append(level)
        deciles.append([p*100, area])
    meta['deciles'] = deciles
    
    for l in levels: print(l)

# make latitude histogram
    nside = healpy.get_nside(healpix_data)
    allipix = numpy.arange(0, len(healpix_data))
    (th, ph) = healpy.pix2ang(nside, allipix)

    h = []
    max = 0
    for i in range(len(healpix_data)):
        if healpix_data[i] > max:
            max = healpix_data[i]
            imax = i
        if healpix_data[i] > levels[2]:
            ira  = int(ph[i]*radian/5.0)
            idec = int(th[i]*radian/5.0)
            pair = [ira, idec]
            if pair not in h:
                h.append(pair)

    print(h)
    meta['histogram'] = h
    
# make countours
    ngrid = 800
    apointRA  = ph[imax]*radian
    apointDec = 90 - th[imax]*radian
    meta['apointRA'] = apointRA
    meta['apointDec'] = apointDec
    print('max prob %f %f' % (apointRA, apointDec))

    theta = numpy.linspace(0, numpy.pi, ngrid)
    phi = numpy.linspace(0, 2*numpy.pi, 2*ngrid)
    p,t = numpy.meshgrid(phi, theta)
    data_interpolated_to_grid = healpy.pixelfunc.get_interp_val(healpix_data, t, p)
    contourplot = plt.contour(p, t, data_interpolated_to_grid, levels=levels)
    numcontours = 0
    contours = []
    for n, c in enumerate(contourplot.collections):
        color = hsv2rgb(n * 1.0 / len(levels), 1.0, 1.0)
        for contourPath, path in enumerate(c.get_paths()):
            plist = []
            for v in path.vertices:
                plist.append([v[0]*radian, 90-v[1]*radian])
            spaths = splitPaths(plist)
            for sp in spaths:
                contours.append({"name":"%d-percentile"%((n+1)*10), "color":color, "coords":sp})
#            contours.append({"name":"%d-percentile"%((n+1)*10), "color":color, "coords":plist})
                numcontours+=1
    print("made %d polylines" % numcontours)

# galaxies from Glade
    gc = galaxyCatalog.gc(glade_filename)
    gc.info()

    de = gc.declination
    ra = gc.right_ascension
    name = numpy.array(gc.name)
    print("has %d sources" % len(ra))

    ph_at_sources = ra/radian
    th_at_sources = (90 - de)/radian

    skymap_at_sources = numpy.zeros(len(ra))
    weighted_sources = numpy.zeros(len(ra))

# distribution of log distance
    ldc = gc.log_distance_centres
    dh =  gc.distance_hist

    if dim == 2:
        distance = gc.distance
        for i in range(len(ra)):
            ipix = healpy.ang2pix(nside, th_at_sources[i], ph_at_sources[i])
            skymap_at_sources[i] = healpix_data[ipix]
            weighted_sources[i] = skymap_at_sources[i]

    if dim == 3:
        distance = gc.distance
        distance_pdf = numpy.zeros(len(ra))
        for i in range(len(ra)):
            ipix = healpy.ang2pix(nside, th_at_sources[i], ph_at_sources[i])
            skymap_at_sources[i] = healpix_data[ipix]
            norm  = distnorm[ipix]
            sigma = distsigma[ipix]
            mu    = distmu[ipix]
            if math.isnan(distance[i]):
                distance_pdf[i] = 0.0
                for k in range(len(ldc)):
                    d = math.pow(10.0, ldc[k])
                    r = math.exp(-(d - mu)**2 / (2*sigma**2)) * norm * (d**2)
                    distance_pdf[i] += r*dh[k]
            else:
                d = distance[i]
                distance_pdf[i] = math.exp(-(d - mu)**2 / (2*sigma**2)) * norm * (d**2)
            weighted_sources[i] = skymap_at_sources[i] * distance_pdf[i]

    weighted_sources = numpy.array(weighted_sources)
    maxw = numpy.max(weighted_sources)
    sumw = numpy.sum(weighted_sources)
    print('max weight = ', maxw)

    index = numpy.argsort(-weighted_sources)
    
    num = len(index)
    if num > 200: num = 200 
    selected_sources = (index[:num])
    print("Selected %d galaxies" % len(selected_sources))

    sources = []
    for de, ra, nm, w, distance in zip(
        de[selected_sources],
        ra[selected_sources],
        name[selected_sources], 
        weighted_sources[selected_sources],
        distance[selected_sources]):
        
        absw = w/sumw           # weight compared to total weight
        sw = math.sqrt(w/maxw)  # normalized sqrt of weight
        if sw < 1.e-3: continue
        if math.isnan(distance): distance = 0.0
        sources.append({"coords": [ra, de], "name":nm, "sw":sw, "absw":absw, "distance":distance})
        n += 1

    print("Sources complete with %d" % n)

    print("Language = ",language)
    return make_json(meta, contours, sources)

def make_json(meta, contours, sources):
    print("building json")
    return json.dumps({"meta":meta, "contours":contours, "sources": sources})

def get_level(m, p):
    whole_sky = radian*radian*4*numpy.pi
    ms = numpy.sort(m, axis=None)
    mr = ms[::-1]

    sum = 0.0
    i = 0
    while sum < p and i < len(mr):
        sum += mr[i]
        i += 1
    if i == 0: return 0.0
    level = mr[i - 1]
    area = i*whole_sky/len(mr)
    return (level, area)

def hsv2rgb(h, s, v):
    r, g, b = hsv_to_rgb(h, s, v)
    r = int(r * 255)
    g = int(g * 255)
    b = int(b * 255)
    return "%02x%02x%02x" % (r, g, b)

if __name__ == "__main__":
    import os, sys
    if len(sys.argv) < 2:
        print("Usage: python skymapInfo.py fitsfiles")
        sys.exit()

    infile = sys.argv[1]
    if os.path.isfile(infile):   # single file .../fits/name/123.fits --> .../json/name/123.json
        outfile = infile.replace('fits.gz', 'json').replace('fits','json')
        if outfile == infile: 
            outfile = infile.replace('fits', 'json')
        makeFile(infile, outfile, 'json')

    if os.path.isdir(infile):   # whole collection .../fits/name/123.fits --> .../json/name/123.json
        print("directory")
        for file in os.listdir(infile):
            if file.endswith('.fits') or file.endswith('.fits.gz'):
                name = infile + file.replace('.fits.gz', '').replace('.fits', '')
                makeFile(infile+'/'+file, name + '.json')
