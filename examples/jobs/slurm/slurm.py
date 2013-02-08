#!/usr/bin/env python2.4
# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

'''This example runs some SLURM commands

   If something doesn't work as expected, try to set 
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.

   If you think you have encountered a defect, please 
   report it at: https://github.com/saga-project/saga-python/issues
'''

__author__    = "Ashley Zebrowski"
__copyright__ = "Copyright 2013, Ashley Zebrowski"
__license__   = "MIT"

import sys, time
import saga
import os
import logging
import subprocess

TEMP_FILENAME = "test.txt" # filename to create and use for testing
#TEMP_DIR      = "/irods_test_dir/" #directory to create and use for testing

def main(args):
    try:
        # create a job service for the local machine. both, 'fork' and 
        # 'local' schemes trigger the local job adaptor. 
        js = saga.job.Service("slurm://localhost")
        
        # describe our job
        jd = saga.job.Description()

        # environment, executable & arguments
        jd.environment = {'CATME':'10'}       
        jd.executable  = '/bin/sleep'
        jd.arguments   = ['$CATME']
        
        # output options (will just be empty files for /bin/sleep)
        jd.output = "saga_localjob.stdout"
        jd.error  = "saga_localjob.stderr"

        # create the job (state: New)
        catjob = js.create_job(jd)

        # check our job's id and state
        print "Job ID    : %s" % (catjob.id)
        print "Job State : %s" % (catjob.state)

        print "\n...starting job...\n"
        catjob.run()

        print "Job ID    : %s" % (catjob.id)
        print "Job State : %s" % (catjob.state)

        print "\nListing active jobs: "
        for job in js.list():
            print " * %s" % job

        # wait for our job to complete
        print "\n...waiting for job...\n"
        catjob.wait()

        print "Job State : %s" % (catjob.state)
        print "Exitcode  : %s" % (catjob.exit_code)
        

    except Exception, ex:
        logging.exception("An error occured while executing the test script!"
                          "Please run with SAGA_VERBOSE=4 set in the"
                          "environment for debug output.  %s"
                          % (str(ex)))
        print " *** %s" % saga.utils.exception.get_traceback()
        sys.exit(-1)

    print "SLURM test script finished execution"

if __name__ == "__main__":
    sys.exit(main(sys.argv))
