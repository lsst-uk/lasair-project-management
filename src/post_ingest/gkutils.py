def dbConnect(lhost, luser, lpasswd, ldb, lport=3306, quitOnError=True):
   #import MySQLdb
   import mysql.connector as MySQLdb

   try:
      conn = MySQLdb.connect (host = lhost,
                              user = luser,
                            passwd = lpasswd,
                                db = ldb,
                              port = lport)
   except MySQLdb.Error as e:
      print(("Error %d: %s" % (e.args[0], e.args[1])))
      if quitOnError:
         sys.exit (1)
      else:
         conn=None

   return conn

# 2013-02-04 KWS Create an object from a dictionary.
class Struct:
    """Create an object from a dictionary. Ensures compatibility between raw scripted queries and Django queries."""
    def __init__(self, **entries): 
        self.__dict__.update(entries)

# 2017-11-02 KWS Quick and dirty code to clean options dictionary as extracted by docopt.
def cleanOptions(options):
    cleanedOpts = {}
    for k,v in list(options.items()):
        # Get rid of -- and <> from opts
        cleanedOpts[k.replace('--','').replace('<','').replace('>','')] = v

    return cleanedOpts

def readGenericDataFile(filename, delimiter = ' ', skipLines = 0, fieldnames = None, useOrderedDict = False):
   import csv
   from collections import OrderedDict

   # Sometimes the file has a very annoying initial # character on the first line.
   # We need to delete this character or replace it with a space.

   if type(filename).__name__ == 'file' or type(filename).__name__ == 'instance':
      f = filename
   else:
      f = open(filename)

   if skipLines > 0:
      [f.readline() for i in range(skipLines)]

   # We'll assume a comment line immediately preceding the data is the column headers.

   # If we already have a header line, skip trying to read the header
   if not fieldnames:
      index = 0
      header = f.readline().strip()
      if header[0] == '#':
         # Skip the hash
         index = 1

      if delimiter == ' ': # or delimiter == '\t':
         # Split on whitespace, regardless of however many spaces or tabs between fields
         fieldnames = header[index:].strip().split()
      else:
         fieldnames = header[index:].strip().split(delimiter)

   # 2018-02-12 KWS Strip out whitespace from around any fieldnames
   fieldnames = [x.strip() for x in fieldnames]
   # The file pointer is now at line 2

   t = csv.DictReader(f, fieldnames = fieldnames, delimiter=delimiter, skipinitialspace = True)

   data = []
   for row in t:
      if useOrderedDict:
          data.append(OrderedDict((key, row[key]) for key in fieldnames))
      else:
          data.append(row)

   # Only close the file if we opened it in the first place
   if not (type(filename).__name__ == 'file' or type(filename).__name__ == 'instance'):
      f.close()

   # We now have the data as a dictionary.
   return data

# Utilities file.  The following import are used so often I've placed them at
# the top of the file.  Other imports are executed only when needed.

import time
import os, sys
from datetime import datetime
import math
import warnings
import re
from operator import itemgetter

warnings.filterwarnings('ignore', '.*the sets module is deprecated.*', DeprecationWarning, 'MySQLdb')


def bin(x, digits=0):
    oct2bin = ['000','001','010','011','100','101','110','111']
    binstring = [oct2bin[int(n)] for n in oct(x)]
    return ''.join(binstring).lstrip('0').zfill(digits)


def ra_to_sex (ra, delimiter = ':', decimalPlaces = 2):

   if decimalPlaces < 2:
      decimalPlaces = 2

   if decimalPlaces > 4:
      decimalPlaces = 4

   accuracy = 10**decimalPlaces

   if ra < 0.0:
      ra = ra + 360.0

   ra_hh   = int(ra/15)
   ra_mm   = int((ra/15 - ra_hh)*60)
   ra_ss   = int(((ra/15 - ra_hh)*60 - ra_mm)*60)
   ra_ff  = int((((ra/15 - ra_hh)*60 - ra_mm)*60 - ra_ss)*accuracy)

   return ('%02d' %ra_hh + delimiter + '%02d' %ra_mm + delimiter + '%02d' %ra_ss + '.' + '%0*d' % (decimalPlaces, ra_ff))


def dec_to_sex (dec, delimiter = ':', decimalPlaces = 1):

   if decimalPlaces < 1:
      decimalPlaces = 1

   if decimalPlaces > 3:
      decimalPlaces = 3

   accuracy = 10**decimalPlaces

   if (dec >= 0):
      hemisphere = '+'
   else:
      # Unicode minus sign - should be treated as non-breaking by browsers
      hemisphere = '-'
      dec *= -1

   dec_deg = int(dec)
   dec_mm  = int((dec - dec_deg)*60)
   dec_ss  = int(((dec - dec_deg)*60 - dec_mm)*60)
   dec_f  = int(((((dec - dec_deg)*60 - dec_mm)*60) - dec_ss)*accuracy)

   return (hemisphere + '%02d' %dec_deg + delimiter + '%02d' %dec_mm + delimiter + '%02d' %dec_ss + '.' + '%0*d' % (decimalPlaces, dec_f))


def coords_dec_to_sex (ra, dec, delimiter = ':', decimalPlacesRA = 2, decimalPlacesDec = 1):

   return(ra_to_sex(ra,delimiter, decimalPlaces = decimalPlacesRA), dec_to_sex(dec,delimiter, decimalPlacesDec))


# 2015-03-18 KWS Added N and E offset calculator (thanks Dave)
def getOffset(ra1, dec1, ra2, dec2):
   '''Work out N-S, E-W separations (object 1 relative to 2)'''
   north = -(dec1 - dec2) * 3600.0
   east = -(ra1 - ra2) * math.cos(math.radians((dec1 + dec2)) / 2.) * 3600.0
   offset = {'N': north, 'E': east}

   return offset

def ra_in_decimal_hours(ra):

   return(ra/15.0)

# Base-26 converter - for local QUB PS1 IDs
def baseN(num, base=26, numerals="abcdefghijklmnopqrstuvwxyz"):
   if num == 0:
       return numerals[0]

   if num < 0:
       return '-' + baseN((-1) * num, base, numerals)

   if not 2 <= base <= len(numerals):
       raise ValueError('Base must be between 2-%d' % len(numerals))

   left_digits = num // base
   if left_digits == 0:
       return numerals[num % base]
   else:
       return baseN(left_digits, base, numerals) + numerals[num % base]

# Base 26 number padded with base 26 zeroes (a)
def base26(num):
   if num < 0:
      raise ValueError('Number must be positive or zero')

   return baseN(num).rjust(3,'a')


# 2013-02-27 KWS Added converter to get base 10 number back from base 26
def base26toBase10(b26number):
    """Convert from Base 26 to Base 10. Only accept lowercase letters"""
    numerals="abcdefghijklmnopqrstuvwxyz"
    import re
    if re.search('^[a-z]+$', b26number):
        b26number = b26number[::-1]
        b10number = 0
        for i in range(len(b26number)):
            b10number += numerals.index(b26number[i])*(26 ** i)
    else:
        return -1

    return b10number


def getCurrentJD():
   jd = time.time()/86400.0+2440587.5
   return jd

def getCurrentMJD():
   jd = getCurrentJD()
   mjd = jd-2400000.5
   return mjd


def getJDfromMJD(mjd):
   jd = mjd + 2400000.5
   return jd
   

def getDateFromMJD(mjd, fitsFormat=False):
   unixtime = (mjd + 2400000.5 - 2440587.5) * 86400.0;
   theDate = datetime.utcfromtimestamp(unixtime)
   stringDate = theDate.strftime("%Y-%m-%d %H:%M:%S")
   if fitsFormat == True:
      stringDate = stringDate.replace(' ','T')

   return stringDate


# 2012-03-26 KWS Added function to convert from date to MJD
def getMJDFromSqlDate(sqlDate):
   mjd = None

   try:
      year, month, day = sqlDate[0:10].split('-')
      hours, minutes, seconds = sqlDate[11:19].split(':')
      t = (int(year), int(month), int(day), int(hours), int(minutes), int(seconds), 0, 0, 0)
      unixtime = int(time.mktime(t))
      mjd = unixtime/86400.0 - 2400000.5 + 2440587.5
   except ValueError, e:
      mjd = None
      print "String is not in SQL Date format."

   return mjd

def getUnixTimeFromSQLDate(sqlDate):
   unixTime = None

   try:
      year, month, day = sqlDate[0:10].split('-')
      hours, minutes, seconds = sqlDate[11:19].split(':')
      t = (int(year), int(month), int(day), int(hours), int(minutes), int(seconds), 0, 0, 0)
      unixTime = int(time.mktime(t))
   except ValueError, e:
      unixTime = None
      print "String is not in SQL Date format."

   return unixTime

def getSQLDateFromUnixTime(unixTime):
   sqlDate = None
   try:
      sqlDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(unixTime)))
   except ValueError, e:
      sqlDate = None
      print "Unix time must be an integer."
   return sqlDate

def getDateFractionMJD(mjd, delimiter = ' ', decimalPlaces = 2):
   floatWidth = decimalPlaces + 3 # always have 00.00 or 00.000 or 00.0000, etc
   unixtime = (mjd + 2400000.5 - 2440587.5) * 86400.0;
   theDate = datetime.utcfromtimestamp(unixtime)
   dateString = theDate.strftime("%Y:%m:%d:%H:%M:%S")
   (year, month, day, hour, min, sec) = dateString.split(':')
   dayFraction = int(day) + int(hour)/24.0 + int(min)/(24.0 * 60.0) + int(sec)/(24.0 * 60.0 * 60.0)
   dateFraction = "%s%s%s%s%0*.*f" % (year, delimiter, month, delimiter, floatWidth, decimalPlaces, dayFraction)
   return dateFraction


def sexToDec (sexv, ra = False, delimiter = ':'):
   # Note that the approach below only works because there are only two colons
   # in a sexagesimal representation.
   degrees = 0
   minutes = 0
   seconds = 0
   decimalDegrees = None
   sgn = 1

   try:
      # Look for a minus sign.  Note that -00 is the same as 00.

      (degreesString, minutesString, secondsString) = sexv.split(delimiter)

      if degreesString[0] == '-':
         sgn = -1
      else:
         sgn = 1

      degrees = abs(float(degreesString))
      minutes = float(minutesString)
      seconds = float(secondsString)
      if ra:
         degrees *= 15.0
         minutes *= 15.0
         seconds *= 15.0

      decimalDegrees = (degrees + (minutes / 60.0) + (seconds / 3600.0)) * sgn
      if not ra and (decimalDegrees < -90.0 or decimalDegrees > 90.0):
         decimalDegrees = None
      elif ra and (decimalDegrees < 0.0 or decimalDegrees > 360.0):
         decimalDegrees = None
   except ValueError:
      # Just in case we're passed a dodgy string
      decimalDegrees = None

   return decimalDegrees


def coords_sex_to_dec (ra, dec, delimiter = ':'):

   return(sexToDec(ra, ra=True ,delimiter=delimiter), sexToDec(dec, ra=False, delimiter=delimiter))


def calculate_cartesians(ra, dec):
   ra = math.radians(ra)
   dec = math.radians(dec)
   cos_dec = math.cos(dec)
   cx = math.cos(ra) * cos_dec
   cy = math.sin(ra) * cos_dec
   cz = math.sin(dec)

   cartesians = (cx, cy, cz)
   return cartesians


pi = (4*math.atan(1.0))
DEG_TO_RAD_FACTOR = pi/180.0
RAD_TO_DEG_FACTOR = 180.0/pi

def getAngularSeparation(ra1, dec1, ra2, dec2):
   """
   Calculate the angular separation between two objects.  If either set of
   coordinates contains a colon, assume it's in sexagesimal and automatically
   convert into decimal before doing the calculation.
   """

   if ':' in str(ra1):
      ra1 = sexToDec(ra1, ra=True)
   if ':' in str(dec1):
      dec1 = sexToDec(dec1, ra=False)
   if ':' in str(ra2):
      ra2 = sexToDec(ra2, ra=True)
   if ':' in str(dec2):
      dec2 = sexToDec(dec2, ra=False)

   # 2013-10-20 KWS Always make sure that the ra and dec values are floats

   ra1 = float(ra1)
   ra2 = float(ra2)
   dec1 = float(dec1)
   dec2 = float(dec2)

   angularSeparation = None

   if ra1 is not None and ra2 is not None and dec1 is not None and dec2 is not None:

      aa  = (90.0-dec1)*DEG_TO_RAD_FACTOR
      bb  = (90.0-dec2)*DEG_TO_RAD_FACTOR
      cc  = (ra1-ra2)*DEG_TO_RAD_FACTOR
      one = math.cos(aa)*math.cos(bb)
      two = math.sin(aa)*math.sin(bb)*math.cos(cc)

      # Because acos() returns NaN outside the ranges of -1 to +1
      # we need to check this.  Double precision decimal places
      # can give values like 1.0000000000002 which will throw an
      # exception.

      three = one+two

      if (three > 1.0):
         three = 1.0
      if (three < -1.0):
         three = -1.0

      angularSeparation = math.acos(three)*RAD_TO_DEG_FACTOR*3600.0

   return angularSeparation

def bruteForceGenericConeSearch(filename, coordinatePairs, radius, delimiter = '\t', inputDelimiter = '\t', raIndex = 'ra', decIndex = 'dec', minradius = 0.0, catalogue = []):
   """
   Pass a generic text (csv) catalog for searching. Assumes for now that the catalog is space separated.
   Default output is a tab separated header and tab separated data items.
   """
   import csv 

   # Sometimes the file has a very annoying initial # character on the first line.
   # We need to delete this character or replace it with a space.

   # Check annulus parameter
   if minradius > radius:
      # The min radius can't be greater than the radius
      minradius = 0.0

   if catalogue:
       cols = catalogue[0].keys()
       t = catalogue
   else:
       f = open(filename)

       index = 0
       header = f.readline().strip()
       if header[0] == '#':
          # Skip the hash
          index = 1

       fieldnames = header[index:].strip().split(inputDelimiter)

       # The file pointer is now at line 2

       t = csv.DictReader(f, fieldnames = fieldnames, delimiter=inputDelimiter, skipinitialspace = True)
       cols = t.fieldnames

   header = ''
   tableRow = ''
   body = []

   basename = os.path.basename(filename)

   resultsTable = []

   i = 0
   for coord in coordinatePairs:
      for row in t:
         raCat = row[raIndex]
         decCat = row[decIndex]

         separation = getAngularSeparation(coord[0], coord[1], raCat, decCat)
         if separation < radius and separation > minradius:
            # Add the returned row (there may be more than one) and the object counter
            resultsTable.append([row, separation, i])
      i += 1


   if resultsTable:
      # Column headers
      for name in cols:
         # Change names of the RA/DEC columns so the DS9 catalogue reader can read them
         header += "%s%s" % (name, delimiter)
      header += "%s%s%s%s%s" % ('filename', delimiter, 'separation', delimiter, 'object_id')

      # Data

      for row in resultsTable:
         tableRow = ''
         for name in cols:
            tableRow += "%s%s" % (row[0][name], delimiter)
            #print "%s\t" % col,

         tableRow += "%s%s%s%s%s" % (basename, delimiter, row[1], delimiter, row[2])
         body.append(tableRow)

   if not catalogue:
       f.close()

   return header, body


# J2000 to Galactic coordinates calculation
# This code extracted from a JavaScript utility
# and converted into Python


J2000toGalactic = [-0.054875539390, -0.873437104725, -0.483834991775,
                   +0.494109453633, -0.444829594298, +0.746982248696,
                   -0.867666135681, -0.198076389622, +0.455983794523]

# 2015-01-04 KWS More accurate values from Liu et al Reconsidering the galactic coordinate system, 2010
#                When implemented, don't forget to alter the C++ code equivalent

# 2015-01-04 KWS Convert back from galactic to J2000. Is this not just a transposition of the original matrix??
GalactictoJ2000 = [-0.054875539390, +0.494109453633, -0.867666135681,
                   -0.873437104725, -0.444829594298, -0.198076389622,
                   -0.483834991775, +0.746982248696, +0.455983794523]

# 2015-06-13 KWS Ecliptic to Equatorial matrix
ETA = math.radians(23.4333333333333)

EcliptictoJ2000 = [1.0, 0.0, 0.0,
                   0.0, math.cos(ETA), -math.sin(ETA),
                   0.0, math.sin(ETA), math.cos(ETA)]

# returns a radec array of two elements
def transform ( coords, matrix ):
   pi = math.pi

   r0 = calculate_cartesians(coords[0], coords[1]) 

   s0 = [
         r0[0]*matrix[0] + r0[1]*matrix[1] + r0[2]*matrix[2], 
         r0[0]*matrix[3] + r0[1]*matrix[4] + r0[2]*matrix[5], 
         r0[0]*matrix[6] + r0[1]*matrix[7] + r0[2]*matrix[8]
        ] 
 
   r = math.sqrt ( s0[0]*s0[0] + s0[1]*s0[1] + s0[2]*s0[2] )

   result = [ 0.0, 0.0 ]
   result[1] = math.asin ( s0[2]/r )

   cosaa = ( (s0[0]/r) / math.cos(result[1] ) )
   sinaa = ( (s0[1]/r) / math.cos(result[1] ) )
   result[0] = math.atan2 (sinaa,cosaa)
   if result[0] < 0.0:
      result[0] = result[0] + pi + pi

   # Convert to degrees

   result[0] = math.degrees(result[0])
   result[1] = math.degrees(result[1])

   return result


# 2015-01-06 KWS Hammer projection calculation - derived from a MATLAB routine extracted
#                from http://www.astro.caltech.edu/~eran/MATLAB/Map.html

def pr_hammer(Long,Lat,R):
   Long = math.radians(Long)
   Lat = math.radians(Lat)

   X = 2.0*R*math.sqrt(2)*math.cos(Lat)*math.sin(Long/2)/math.sqrt(1+math.cos(Lat)*math.cos(Long/2))
   Y = R*math.sqrt(2)*math.sin(Lat)/math.sqrt(1+math.cos(Lat)*math.cos(Long/2))

   return X,Y


# 2012-03-07 KWS Created redshiftToDistance calculator based on our C++ code,
#                which is itself based on Ned Wright's Cosmology Calculator code.

def redshiftToDistance(z):

   # Cosmological Parameters (to be changed if required)
   WM = 0.3           # Omega_matter
   WV = 0.7           # Omega_vacuum
   H0 = 70.0           # Hubble constant (km s-1 Mpc-1)

   # Other variables
   h = H0/100.0
   WR = 4.165E-5/(h*h)     # Omega_radiation
   WK = 1.0-WM-WV-WR       # Omega_curvature = 1 - Omega(Total)
   c = 299792.458          # speed of light (km/s)

   # Arbitrarily set the values of these variables to zero just so we can define them.

   DCMR  = 0.0             # comoving radial distance in units of c/H0
   DCMR_Mpc = 0.0          # comoving radial distance in units of Mpc
   DA = 0.0                # angular size distance in units of c/H0
   DA_Mpc = 0.0            # angular size distance in units of Mpc
   DA_scale = 0.0          # scale at angular size distance in units of Kpc / arcsec
   DL = 0.0                # luminosity distance in units of c/H0
   DL_Mpc = 0.0            # luminosity distance in units of Mpc
   DMOD = 0.0              # Distance modulus determined from luminosity distance
   a = 0.0                 # 1/(1+z), the scale factor of the Universe

   az = 1.0/(1.0+z)        # 1/(1+z), for the given redshift

   # Compute the integral over a=1/(1+z) from az to 1 in n steps
   n = 1000
   for i in range(n):
      a = az+(1.0-az)*(i+0.5)/n
      adot = math.sqrt(WK+ (WM/a) + (WR/(math.pow(a,2))) +(WV*math.pow(a,2)))
      DCMR = DCMR + 1.0/(a*adot)

   DCMR = (1.0-az)*DCMR/n           # comoving radial distance in units of c/H0
   DCMR_Mpc = (c/H0)*DCMR           # comoving radial distance in units of Mpc

   # Tangental comoving radial distance
   x = math.sqrt(abs(WK))*DCMR
   if x > 0.1:
      if WK > 0.0:
         ratio = 0.5*(math.exp(x)-math.exp(-x))/x
      else:
         ratio = math.sin(x)/x
   else:
      y = math.pow(x,2)
      if WK < 0.0:
         y=-y
      ratio = 1 + y/6.0 + math.pow(y,2)/120.0

   DA = az*ratio*DCMR               #angular size distance in units of c/H0
   DA_Mpc = (c/H0)*DA               #angular size distance in units of Mpc
   DA_scale = DA_Mpc/206.264806     #scale at angular size distance in units of Kpc / arcsec
   DL = DA/math.pow(az,2)                #luminosity distance in units of c/H0
   DL_Mpc = (c/H0)*DL               #luminosity distance in units of Mpc
   DMOD = 5*math.log10(DL_Mpc*1e6)-5     #Distance modulus determined from luminosity distance


   results = \
   {
      "dcmr_mpc": DCMR_Mpc,
      "da_mpc": DA_Mpc,
      "da_scale": DA_scale,
      "dl_mpc": DL_Mpc,
      "dmod": DMOD,
      "z" : z
   }

   return results



# 2012-10-04 KWS Moved enum to utils.py
def enum(**enums):
   return type('Enum', (), enums)

def ra_dec_id(ra, dec):
   id = 1000000000000000000L

   # Calculation from Decimal Degrees:

   ra_hh   = int(ra/15)
   ra_mm   = int((ra/15 - ra_hh)*60.0)
   ra_ss   = int(((ra/15 - ra_hh)*60.0 - ra_mm)*60.0)
   ra_fff  = int((((ra/15 - ra_hh)*60.0 - ra_mm)*60.0 - ra_ss)*1000.0)

   h = None

   if (dec >= 0):
      h = 1
   else:
      h = 0
      dec = dec * -1

   dec_deg = int(dec)
   dec_mm  = int((dec - dec_deg)*60.0)
   dec_ss  = int(((dec - dec_deg)*60.0 - dec_mm)*60.0)
   dec_ff  = int(((((dec - dec_deg)*60.0 - dec_mm)*60.0) - dec_ss)*100.0)

   id += (ra_hh *   10000000000000000L)
   id += (ra_mm *     100000000000000L)
   id += (ra_ss *       1000000000000L)
   id += (ra_fff *         1000000000L)

   id += (h *               100000000L)
   id += (dec_deg *           1000000L)
   id += (dec_mm *              10000L)
   id += (dec_ss *                100L)
   id += dec_ff

   return id


def getReducedLC(filterdata, recurrencePeriod = 0.5):
    """Return a reduced array per filter. We need to do this because of PS1 skycell overlaps.
       The recurrence period by default is half a day, counting forwards."""

    # This method exists because of skycell overlaps.  We're dealing with the same
    # data, so produce a MEAN error rather than adding them in quadrature.

    import numpy as n

    # With overlapping skycells or multiple samples per filter
    # we only want the average mag vs average mjd

    filterAvgArray = []
    mjdMax = 0
    firstPass = True
    mags = []
    mjds = []
    magerrs = []
    for mjd, mag, magerr in filterdata:
        # Create a new g array with mean mjd, mean mag, mean error.

        if firstPass:
            mjdMax = mjd + recurrencePeriod
            firstPass = False

        if mjd > mjdMax: # which it can never be 1st time round
            mjdAvg = n.array(mjds).mean()
            magAvg = n.array(mags).mean()
            magErrAvg = n.array(magerrs).mean()
            filterAvgArray.append([mjdAvg, magAvg, magErrAvg])
            mjds = []
            mags = []
            magerrs = []
            mjdMax = mjd + recurrencePeriod

        mjds.append(mjd)
        mags.append(mag)
        magerrs.append(magerr)

    # Clean up the last set of mjds and mags
    if mjds:
        mjdAvg = n.array(mjds).mean()
        magAvg = n.array(mags).mean()
        magErrAvg = n.array(magerrs).mean()
        filterAvgArray.append([mjdAvg, magAvg, magErrAvg])

    return filterAvgArray


# 2013-02-04 KWS Added utility to get colour from two different sets of filter data
def getColour(cData1, cData2, dateDiffLimit, interpolate = False):
    '''Create an array of colour1 - colour2 points vs MJD'''
    #Algorithm:

    # Start with colour1
    # Some colour data is intra-day, but need to choose ONE recurrence of colour1 and ONE
    # recurrence of colour2 because of skycell overlaps. Probably choose mean of both.

    c1c2Colour = []

    # We can't do a colour plot if one of the filters is missing.
    if not cData1 or not cData2:
        return c1c2Colour

    # The filter arrays should be ordered by MJD. This means we should only need to walk forward
    # through the array.

    reducedC1 = getReducedLC(cData1)
    reducedC2 = getReducedLC(cData2)

    c1Dates = [row[0] for row in reducedC1]
    c2Dates = [row[0] for row in reducedC2]

    # OK We now have 2 reduced lightcurves.  Now subtract them.


    for c1idx, c1Date in enumerate(c1Dates):
        # Find the nearest r value to each g MJD
        c2idx = min(range(len(c2Dates)), key=lambda i: abs(c2Dates[i]-c1Date))

        if interpolate:
            # We want attempt to linearly interpolate, but I'm not sure
            # how I'll do that yet... I guess that if the nearest value is
            # within a specified period that's too far to average and the value
            # comes from a point in FRONT of the current date, we do a linear
            # interpolation from the date previous.

            pass
            
            #xx = n.array([parametri1['r'],parametri1['z']])
            #yy2 = n.array([cm['r'],cm['z']])
            #maginterp2=n.interp(parametri1['i'],xx,yy2)

        if abs(c1Date - c2Dates[c2idx]) < dateDiffLimit:
            colour = reducedC1[c1idx][1] - reducedC2[c2idx][1]
            avgDate = (reducedC1[c1idx][0] + reducedC2[c2idx][0])/2
            error = math.sqrt(reducedC1[c1idx][2] * reducedC1[c1idx][2] + reducedC2[c2idx][2] * reducedC2[c2idx][2])
            c1c2Colour.append([avgDate, colour, error])
            #print avgDate, colour

    return c1c2Colour


def getColourStats(colourData):

    import numpy as n

    x = n.array(colourData)[:,0]
    y = n.array(colourData)[:,1]

    meanColour = y.mean()

    # We can rewrite the line equation as y = Ap, where A = [[x 1]]
    # and p = [[m], [c]]. Now use lstsq to solve for p

    A = n.vstack([x, n.ones(len(x))]).T

    # In this case the gradient is the colour evolution
    gradient, intercept = n.linalg.lstsq(A, y)[0]

    return meanColour, gradient


# 2013-02-15 KWS Added RMS scatter calculation code.  The objectInfo object
#                is a list of dictionaries with at least "RA" and "DEC" keys.

def calcRMS(objectInfo, avgRa, avgDec, rms = None):
   sep = sepsq = 0

   for objectRow in objectInfo:
      delra = (avgRa-objectRow["RA"]) * math.cos(math.radians(avgDec))
      deldec = avgDec - objectRow["DEC"]
      delra *= 3600
      deldec *= 3600
      sep = math.sqrt(delra**2 + deldec**2)

      if rms:
         if sep < (2 * rms):
            sepsq = sepsq + delra**2 + deldec**2
      else:
         sepsq = sepsq + delra**2 + deldec**2

   rms = math.sqrt(sepsq/len(objectInfo))
   rms = round(rms, 3)
   return rms



def calculateRMSScatter(objectInfo):

   ### PRINT DETECTION INFORMATION & DETERMINE RMS SEPARATION FROM AVERAGE POSITION ###
   # 2017-10-30 KWS Set initial variables to zero, not equal to each other = 0.
   sep = 0
   totalRa = 0
   totalDec = 0
   sepsq = 0

   # Return negative RMS if no objects in the list (shouldn't happen)
   if len(objectInfo) == 0:
      return -1.0

   for objectRow in objectInfo:
      totalRa += objectRow["RA"]
      totalDec += objectRow["DEC"]

   avgRa = totalRa / len(objectInfo)
   avgDec = totalDec / len(objectInfo)

   #print "\taverage RA = %f, average DEC = %f" % (avgRa, avgDec)

   rms = calcRMS(objectInfo, avgRa, avgDec)

   ## APPLY 2-SIGMA CLIPPING TO THE RMS SCATTER -- TO REMOVE OUTLIERS (TWO ITERATIONS) ####

   rms = calcRMS(objectInfo, avgRa, avgDec, rms = rms)
   rms = calcRMS(objectInfo, avgRa, avgDec, rms = rms)

   return avgRa, avgDec, rms


class SetupMySQLSSHTunnel:
    """Setup SSH tunnel to remote MySQL server"""

    tunnelIsUp = False

    def checkServer(self, address, port):
        """Check that the TCP Port we've decided to use for tunnelling is available"""
        # Create a TCP socket
        import socket
        s = socket.socket()
        sys.stderr.write("Attempting to connect to %s on port %s\n" % (address, port))
        try:
            s.connect((address, port))
            sys.stderr.write("Connected to %s on port %s\n" % (address, port))
            return True
        except socket.error, e:
            sys.stderr.write("Connection to %s on port %s failed: %s\n" % (address, port, e))
            return False


    def __init__(self, sshUser, gateway, internalIP, sshPort):
        # Check that the tunnel is up.  If not, setup the tunnel.
        # NOTE: The public key of the user running this code on this machine must be installed on Starbase
        import time, subprocess
        localHostname = "127.0.0.1"
        mysqlPort = 3306

        self.tunnelIsUp = self.checkServer(localHostname, sshPort)

        if not self.tunnelIsUp:
            # Setup the tunnel
            process = subprocess.Popen("ssh -fnN %s@%s -L %d:%s:%d" % (sshUser, gateway, sshPort, internalIP, mysqlPort), shell=True, close_fds=True)
            time.sleep(2)
            self.tunnelIsUp = self.checkServer(localHostname, sshPort)


def slices(s, *args):
    """Code to split a string into fixed fields defined by list of numbers provided in args"""
    position = 0
    for length in args:
        yield s[position:position + length].strip()
        position += length



# 2013-12-12 KWS Added option to return directories only
def find(pattern, path, directoriesOnly = False):
    """Find all files or directories that match a pattern"""
    import os, fnmatch
    result = []
    for root, dirs, files in os.walk(path):
        if directoriesOnly:
            for name in dirs:
                if fnmatch.fnmatch(name, pattern):
                    result.append(os.path.join(root, name))
        else:
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    result.append(os.path.join(root, name))
    return result



# 2015-01-15 KWS Similar to splitList in multiprocessingUtils
def divideList(inputList, nChunks = 1, listChunkSize = None):
   '''Divide a list into chunks'''

   # Break the list of candidates up into the number of CPUs

   listChunks = []

   listLength = len(inputList)
   if listChunkSize is not None:
      if listLength > listChunkSize:
         nChunks = listLength / listChunkSize
         if listLength % listChunkSize:
            # Does the list divide evenly?
            # If not, add 1 to the number of chunks
            nChunks += 1
      else:
         nChunks = 1

   else:
      if nChunks > 1:
         if listLength < nChunks:
            nChunks = listLength
         listChunkSize = int(round((listLength * 1.0) / nChunks))



   if nChunks > 1:
      for i in range(nChunks-1):
         listChunks.append(inputList[i*listChunkSize:i*listChunkSize+listChunkSize])
      # Append the last (probably uneven) chunk.  Might have 1 extra or 1 fewer members.
      listChunks.append(inputList[(i+1)*listChunkSize:])

   else:
      listChunks = [inputList]

   return nChunks, listChunks


def createDS9RegionFile(dirName, data, radec = True, size = 0.02, colour = 'cyan'):
   """
   Generic code for creating a DS9 region file

   **Key Arguments:**
       - ``filename`` -- The filename of the region file
       - ``data``   -- List of rowinates in RA and DEC (decimal degrees) or x and y, plus a label
       - ``radec``    -- Boolean value indicating ra and dec (True) or x and y (False) - not curently used
       - ``label``    -- the object label
       - ``size``     -- size of the markers
       - ``colour``   -- colour of the markers

    **Return:**
       - None

    **Todo**
       - Implement the x, y format

   """

   # Open file and print header

   previousExpName = ''

   rf = None

   for row in data:
      expName = row[0]
      if expName != previousExpName:
         if rf:
            rf.close()
         rf = open(dirName + expName + '.reg', 'w')
         rf.write('# Region file format: DS9 version 4.1\n' +
                  'global color=%s dashlist=8 3 width=1 font="helvetica 14 normal" select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n' % (colour) +
                  'linear\n')
         rf.write('circle %f %f %.2f # color=%s text={%s}\n' % (row[1], row[2], size, colour, row[3]))
         previousExpName = expName
      else:
         rf.write('circle %f %f %.2f # color=%s text={%s}\n' % (row[1], row[2], size, colour, row[3]))
         
   if rf:
      rf.close()

# 2015-06-03 KWS Calculate Position Angle of body 1 wrt body 2
# 2017-08-30 KWS Finally fixed the PA calculation. It should return a value
#                between -90 and +270. (Must be a convention...)

def calculatePositionAngle(ra1, dec1, ra2, dec2):
   """
   Calculate the position angle (bearing) of body 1 w.r.t. body 2.  If either set of
   coordinates contains a colon, assume it's in sexagesimal and automatically
   convert into decimal before doing the calculation.
   """

   if ':' in str(ra1):
      ra1 = sexToDec(ra1, ra=True)
   if ':' in str(dec1):
      dec1 = sexToDec(dec1, ra=False)
   if ':' in str(ra2):
      ra2 = sexToDec(ra2, ra=True)
   if ':' in str(dec2):
      dec2 = sexToDec(dec2, ra=False)

   # 2013-10-20 KWS Always make sure that the ra and dec values are floats

   positionAngle = None

   if ra1 is not None and ra2 is not None and dec1 is not None and dec2 is not None:
      ra1 = math.radians(float(ra1))
      ra2 = math.radians(float(ra2))
      dec1 = math.radians(float(dec1))
      dec2 = math.radians(float(dec2))

      numerator = math.sin(ra1 - ra2)
      denominator = math.cos(dec2) * math.tan(dec1) - math.sin(dec2) * math.cos(ra1 - ra2)
      positionAngle = math.degrees(math.atan(numerator/denominator))
      if denominator < 0.0:
         positionAngle = 180.0 + positionAngle

   return positionAngle

# 2017-08-30 KWS Are the object coordinates inside an ATLAS footprint?
ATLAS_CONESEARCH_RADIUS = 13888.7 # (i.e. sqrt(5280 * 1.86^2 + 5280 * 1.86^2) )
ATLAS_HALF_WIDTH = 5280 * 1.86 # (also = 13888.7 * cos(45) )

def isObjectInsideATLASFootprint(objectRA, objectDec, fpRA, fpDec, separation = None):

    if separation is None:
        # We need to calculate the angular separation. This is expensive, so if
        # we already have this value, use the one sent.
        separation = getAngularSeparation(objectRA, objectDec, fpRA, fpDec)

    pa = calculatePositionAngle(objectRA, objectDec, fpRA, fpDec) + 90.0
    if pa >= 90.0 and pa < 180.0:
        pa = pa - 90.0
    if pa >= 180.0 and pa < 270.0:
        pa = pa - 180.0
    if pa >= 270.0:
        pa = pa - 270.0

    # Bearing (pa) only needs to be between 45 and -45 degrees.
    pa -= 45.0
    pa = abs(pa)
    dist = float(separation) * math.cos(math.radians(45.0 - pa))
    inside = True
    if dist > ATLAS_HALF_WIDTH:
        inside = False

    return inside



# Add a grammatical join. Used by the Transient name server when adding lists of users
# to the comments section.
def grammarJoin(words):
    return reduce(lambda x, y: x and x + ' and ' + y or y,
                 (', '.join(words[:-1]), words[-1])) if words else ''

COORDS_DEC_REGEX = ""
#                       h    h             m    m             s    s     .  f                    (+-)         d    d             m    m             s    s     .  f                 (space      radius)
#COORDS_SEX_REGEX = "^([0-2][0-9])[^0-9]+([0-5][0-9])[^0-9]+([0-5][0-9])(\.[0-9]+){0,1}[^0-9+\-]+([+-]){0,1}([0-9][0-9])[^0-9]+([0-5][0-9])[^0-9]+([0-5][0-9])(\.[0-9]+){0,1}[^0-9 ]{0,1}( ([0-9][0-9]{0,1})){0,1}"
COORDS_SEX_REGEX = "^([0-2][0-9])[^0-9]{0,1}([0-5][0-9])[^0-9]{0,1}([0-5][0-9])(\.[0-9]+){0,1}[^0-9+\-]{0,5}([+-]){0,1}([0-9][0-9])[^0-9]{0,1}([0-5][0-9])[^0-9]{0,1}([0-5][0-9])(\.[0-9]+){0,1}[^0-9 ]{0,1}( +([0-9][0-9]{0,1})){0,1}"
COORDS_SEX_REGEX_COMPILED = re.compile(COORDS_SEX_REGEX)

#                          d.f                                       (+-)d.f                     (space radius)
COORDS_DEC_REGEX = "^([0-9]+(\.[0-9]+){0,1})[^0-9+\-]{0,5}([+-]{0,1}[0-9]+(\.[0-9]+){0,1})[^0-9 ]{0,1}( +([0-9][0-9]{0,1})){0,1}"
COORDS_DEC_REGEX_COMPILED = re.compile(COORDS_DEC_REGEX)

NAME_REGEX = "^(AT|SN|ATLAS|PS([1][\-]){0,1}){0,1}([2][0]){0,1}([0-9][0-9][a-z]{1,4})"
NAME_REGEX_COMPILED = re.compile(NAME_REGEX)

def getObjectNamePortion(inputString):
    # E.g. if the object name is '2016ffx' will return 16ffx
    #      If the object name is 'ATLAS16abc' will return 16abc
    namePortion = None
    name = NAME_REGEX_COMPILED.search(inputString)
    if name:
        prefix = name.group(1) if name.group(1) is not None else ''
        century = name.group(3) if name.group(3) is not None else ''
        namePortion = prefix + century + name.group(4)

    return namePortion

def getCoordsAndSearchRadius(inputString):
    coords = {}
    ra = None
    dec = None
    radius = None

    sex = COORDS_SEX_REGEX_COMPILED.search(inputString)
    if sex:
        hh = sex.group(1)
        mm = sex.group(2)
        ss = sex.group(3)
        ffra = sex.group(4) if sex.group(4) is not None else ''
        sign = sex.group(5) if sex.group(5) is not None else '+'
        deg = sex.group(6)
        mn = sex.group(7)
        sec = sex.group(8)
        ffdec = sex.group(9) if sex.group(9) is not None else ''
        ra = "%s:%s:%s%s" % (hh, mm, ss, ffra)
        dec = "%s%s:%s:%s%s" % (sign, deg, mn, sec, ffdec)
        radius = sex.group(11)
    else: # Try decimal degrees
        decimal = COORDS_DEC_REGEX_COMPILED.search(inputString)
        if decimal:
            ra = decimal.group(1)
            dec = decimal.group(3)
            radius = decimal.group(5)

    coords['ra'] = ra
    coords['dec'] = dec
    coords['radius'] = radius
    return coords

# Return a string representing a float to digits decimal places, truncated.
# Used when we need to truncate an MJD to 3 decimal places for filename.
def truncate(f, digits):
    return ("{:.30f}".format(f))[:-30+digits]

def nullValue(value):
   returnValue = None

   if value and value.strip():
      returnValue = value.strip()

   return returnValue


def floatValue(value):
   import numpy as n
   returnValue = None

   if value:
      try:
         f = float(value)
         if n.isfinite(f):
             returnValue = f
      except ValueError, e:
         pass

   return returnValue


def intValue(value):
   returnValue = None

   if value:
      try:
         if '0x' in value:
             returnValue = int(value, 16)
         else:
             returnValue = int(value)
      except ValueError, e:
         pass

   return returnValue

