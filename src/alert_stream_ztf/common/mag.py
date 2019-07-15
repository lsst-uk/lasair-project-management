import math

def dc_mag(fid, magpsf,sigmapsf, magnr,sigmagnr, magzpsci, isdiffpos):
    """ Compute apparent magnitude from difference magnitude supplied by ZTF
    Parameters
    ----------
    fid
        filter, 1 for green and 2 for red
    magpsf,sigmapsf
        magnitude from PSF-fit photometry, and 1-sigma error
    magnr,sigmagnr
        magnitude of nearest source in reference image PSF-catalog within 30 arcsec
        and 1-sigma error
    magzpsci
        Magnitude zero point for photometry estimates
    isdiffpos
        t or 1 => candidate is from positive (sci minus ref) subtraction; 
        f or 0 => candidate is from negative (ref minus sci) subtraction
    """

    # zero points. Looks like they are fixed.
    ref_zps = {1:26.325, 2:26.275, 3:25.660}
    magzpref = ref_zps[fid]

    # reference flux and its error
    magdiff = magzpref - magnr
    if magdiff > 12.0:
        magdiff = 12.0
    ref_flux = 10**( 0.4* ( magdiff) )
    ref_sigflux = (sigmagnr/1.0857)*ref_flux

    # difference flux and its error
    if magzpsci == 0.0: magzpsci = magzpref
    magdiff = magzpsci - magpsf
    if magdiff > 12.0:
        magdiff = 12.0
    difference_flux = 10**( 0.4* ( magdiff) )
    difference_sigflux = (sigmapsf/1.0857)*difference_flux

    # add or subract difference flux based on isdiffpos
    if isdiffpos == 't': dc_flux = ref_flux + difference_flux
    else:                dc_flux = ref_flux - difference_flux

    # assumes errors are independent. Maybe too conservative.
    dc_sigflux =  math.sqrt( difference_sigflux**2 + ref_sigflux**2 )

    # apparent mag and its error from fluxes
    if dc_flux > 0.0:
        dc_mag = magzpsci - 2.5 * math.log10(dc_flux)
        dc_sigmag = dc_sigflux/dc_flux*1.0857
    else:
        dc_mag = magzpsci
        dc_sigmag = sigmapsf

    return {'dc_mag':dc_mag, 'dc_sigmag':dc_sigmag}

