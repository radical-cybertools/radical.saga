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
import string

TEMP_FILENAME = "test.txt" # filename to create and use for testing
#TEMP_DIR      = "/irods_test_dir/" #directory to create and use for testing

def main(args):
    try:
        # create a job service for the local machine. both, 'fork' and 
        # 'local' schemes trigger the local job adaptor. 

        print "Creating Job Service!"
        js = saga.job.Service("slurm+ssh://stampede")
        #js = saga.job.Service("slurm+ssh://tg803521@login1.stampede.tacc.utexas.edu/")

        print "Testing run_job!"
        #js.run_job("/bin/sleep 5")

        
        print "Creating Job Description!"
        # describe our job
        jd = saga.job.Description()

        # environment, executable & arguments
        jd.environment = {'MYVAR':'10', 'SAGA':'RULES'}    
        jd.executable  = '/bin/sleep'
        jd.arguments   = ['$MYVAR']
        jd.queue = "development"
        jd.name = "SlurmJob"
        jd.job_contact="anz7@rutgers.edu"
        #jd.project = "TG-ASC120003"
        jd.project = "ResSim"
        jd.wall_time_limit = "1"
        jd.number_of_processes=1
        
        # output options (will just be empty files for /bin/sleep)
        jd.output = "saga_slurmjob.stdout"
        jd.error  = "saga_slurmjob.stderr"

        print "Creating Job with Job Description!"
        # create the job (state: New)
        catjob = js.create_job(jd)

        print catjob.get_description()

        # check our job's id and state
        print "Job ID    : %s" % (catjob.id)
        print "Job State : %s" % (catjob.state)

        print "\n...starting job...\n"
        catjob.run()

        print "Job ID    : %s" % (catjob.id)
        print "Job State : %s" % (catjob.state)

        #catjob.cancel()
        #catjob.suspend()
        #catjob.resume()

        print "\nListing active jobs: "
        for job in js.list():
            print " * %s" % job

        # wait for our job to complete
        print "\n...waiting for job...\n"
        catjob.wait()

        print "Job State : %s" % catjob.state
        print "Exitcode  : %s" % catjob.exit_code
        

    except Exception, ex:
        logging.exception("An error occured while executing the test script!"
                          "Please run with SAGA_VERBOSE=4 set in the"
                          "environment for debug output.  %s"
                          % (str(ex)))
        print " *** %s %s" % (saga.utils.exception.get_traceback(), ex)
        sys.exit(-1)

    print "SLURM test script finished execution"

if __name__ == "__main__":
    sys.exit(main(sys.argv))
