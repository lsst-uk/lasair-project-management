#!/usr/bin/env python
# encoding: utf-8
"""
*Given a list of transient location return the sherlock generated crossmatch and classification lists*

:Author:
    David Young

:Date Created:
    February  7, 2020

Usage:
    sherlock_query_example -s <pathToSettingsFile>

Options:
    -h, --help            show this help message
    -v, --version         show version
    -s, --settings        the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from fundamentals import tools
import csv
from sherlock import transient_classifier
import json


def run_sherlock(json_request):
    arguments = None
    # SETUP THE COMMAND-LINE UTIL SETTINGS
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="WARNING",
        options_first=False,
        projectName=False
    )
    arguments, settings, log, dbConn = su.setup()

    name = []
    ra = []
    dec = []
    for obj in json.loads(json_request):
        name.append(obj['name'])
        ra  .append(obj['ra'])
        dec .append(obj['dec'])

    classifier = transient_classifier(
        log=log,
        settings=settings,
        ra=ra,
        dec=dec,
        name=name,
        verbose=0,
        updateNed=False
    )

    classifications, crossmatches = classifier.classify()
    response = {}
    for cm in crossmatches:
        key = cm['transient_object_id']
        if key in response:
            response[key]['matches'].append(cm)
        else:
            response[key] = {}
            response[key]['matches'] = [cm]

    for objname,objclasslist in classifications.items():
        if not objname in response:
            response[objname] = {}
            response[objname]['matches'] = []
        response[objname]['objclass'] = objclasslist[0]

    return json.dumps(response, indent=2)

if __name__ == '__main__':
    """
    *The main function used when ``sherlock_query_example.py`` is run as a single script from the cl*
    """


    # UNPACK REMAINING CL ARGUMENTS USING `EXEC` TO SETUP THE VARIABLE NAMES
    # AUTOMATICALLY
#    a = {}
#    for arg, val in list(arguments.items()):
#        if arg[0] == "-":
#            varname = arg.replace("-", "") + "Flag"
#        else:
#            varname = arg.replace("<", "").replace(">", "")
#        a[varname] = val
#        if arg == "--dbConn":
#            dbConn = val
#            a["dbConn"] = val
#        log.debug('%s = %s' % (varname, val,))

    # OPEN THE ADJACENT CSV FILE AND PLACE TRANSIENT LOCATIONS AND IDs IN A
    # LIST
#    moduleDirectory = os.path.dirname(__file__)
#    if not len(moduleDirectory):
#        moduleDirectory = "."
#    with open(moduleDirectory + "/atlas4_good_list.csv", 'rb') as csvFile:
    json_request = sys.stdin.read()
    json_response = run_sherlock(json_request)
    print(json_response)
