#!/usr/local/bin/python
# encoding: utf-8
"""
*Listen to LVC GCN notice stream and download event skymaps*

:Author:
    David Young
    modified by Roy Williams 11 March

:Date Created:
    March  4, 2019

Usage:
    lvc_gcn_listener [-t] [<pathToSettingsFile>]

Options:
    <pathToSettingsFile>  path to the settings file containing logger settings

    -t, --test            listen to real *and* test notices (~1 event/hr)
    -h, --help            show this help message
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import json
from fundamentals import tools
import requests
import gcn
sys.path.append('/home/roy/lasair/src/alert_stream_ztf/common/')
import settings
import slack_sender

# VARIABLES
downloadPath = "/data/ztf/skymap/"

gcn_host='209.208.78.170'
# gcn_host='45.58.43.186'
# gcn_host='50.116.49.68'
# gcn_host='68.169.57.253'

def main(arguments=None):
    """
    *The main function used when ``lvc_gcn_listener.py`` is run as a single script from the cl*
    """

    # SETUP THE COMMAND-LINE UTIL SETTINGS
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="INFO",
        options_first=False,
        projectName=False
    )
    arguments, settings, log, dbConn = su.setup()

    # UNPACK REMAINING CL ARGUMENTS USING `EXEC` TO SETUP THE VARIABLE NAMES
    # AUTOMATICALLY
    for arg, val in arguments.iteritems():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    listener = gcnListener(
        log=log,
        settings=settings,
        test=testFlag
    )
    listener.listen()

    return


class gcnListener():
    """
    *The GCN listener object*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``test`` -- use test settings for development purposes
    """

    def __init__(
            self,
            log,
            settings=False,
            test=False
    ):
        self.log = log
        log.debug("instansiating a new 'gcnListener' object")
        self.settings = settings
        self.test = test

        return None

    def listen(self):
        """*listen to the GCN notice stream*
        """
        self.log.debug('starting the ``listen`` method')

        # FILTER THE GCN NOTICES TO ONLY LVC NOTICES
        @gcn.handlers.include_notice_types(
            gcn.notice_types.LVC_PRELIMINARY,
            gcn.notice_types.LVC_INITIAL,
            gcn.notice_types.LVC_UPDATE,
            gcn.notice_types.LVC_RETRACTION)
        def process_gcn(payload, root):

            # DECIDE HOW TO RESPOND TO REAL/TEST EVENTS
            if root.attrib['role'] == 'observation':
                pass
            if root.attrib['role'] == 'test':
                if not self.test:
                    return

            # READ ALL OF THE VOEVENT PARAMETERS FROM THE "WHAT" SECTION.
            params = {elem.attrib['name']:
                      elem.attrib['value']
                      for elem in root.iterfind('.//Param')}

            # PRINT ALL PARAMETERS.
            for key, value in params.items():
                print(key, ':', value)

            # DOWNLOAD THE FITS MAP
            if 'skymap_fits' in params:
                local_filename = params['skymap_fits'].split('/')[-1]
                self.log.info(local_filename)
                if local_filename == 'bayestar.fits.gz':
#                    local_filename = 'test_%d.fits.gz' % int(time.time())
                    local_filename = params['GraceID'] + '.fits.gz'
                    self.log.info(local_filename)
                r = requests.get(params['skymap_fits'], allow_redirects=True)
                filename = downloadPath + "/" + local_filename
                self.log.info(filename)
                open(filename, 'wb').write(r.content)
                cmd = "/home/roy/anaconda3/bin/python ./skymapInfo.py " + filename
                self.log.info(cmd)
                os.system(cmd)

                classification = {'BNS':0.0, 'NSBH':0.0, 'BBH':0.0}
                if 'BNS'  in params: classification['BNS']  = params['BNS']
                if 'NSBH' in params: classification['NSBH'] = params['NSBH']
                if 'BBH'  in params: classification['BBH']  = params['BBH']
                # now modify the json file
                filenamejson = filename.replace('fits.gz', 'json').replace('fits','json')
                dict = json.loads(open(filenamejson).read())
                dict['meta']['classification'] = classification
                print(dict['meta'])
                s = json.dumps(dict)
                f = open(filenamejson, 'w')
                f.write(s)
                f.close()

                bbh = int(100*float(classification['BNS']))
                ss = slack_sender.SlackSender(settings.SLACKURL)
                ss.send('New LVC skymap (%d%% prob BNS) at https://lasair.roe.ac.uk/skymap/%s/' % (bbh, params['GraceID']))

        # START THE LISTENER - WILL RECONNECT AUTOMATICALLY IF CONNECTION DROPS
        gcn.listen(handler=process_gcn, host=gcn_host)

        self.log.debug('completed the ``listen`` method')
        return None


if __name__ == '__main__':
    main()
