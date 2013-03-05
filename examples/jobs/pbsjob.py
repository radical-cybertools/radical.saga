#!/usr/bin/env python
# encoding: utf-8

""" This examples shows how to run a job on a remote PBS/TORQUE cluster
    using the 'pbs' job adaptor.

    More information about the saga-python job API can be found at:
    http://saga-project.github.com/saga-python/doc/library/job/index.html
"""

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import sys
import saga


def main():

    try:
        # Create a job service object that represent a remote pbs cluster.
        # The keyword 'pbs' in the url scheme triggers the PBS adaptors
        # and '+ssh' enables PBS remote access via SSH.
        js = saga.job.Service("pbs+ssh://alamo.futuregrid.org")

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = saga.job.Description()
        jd.queue           = 'batch'
        jd.environment     = {'RUNTIME': '10'}
        jd.wall_time_limit = 1 # minutes
        jd.executable      = '/bin/sleep'
        jd.arguments       = ['$RUNTIME']

        # Create a new job from the job description. The initial state of 
        # the job is 'New'.
        sleepjob = js.create_job(jd)

        # check our job's id and state
        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        print "\n...starting job...\n"
        sleepjob.run()

        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        print "\nListing active jobs: "
        for job in js.list():
            print " * %s" % job

        # disconnect / reconnect
        sleebjob_clone = js.get_job(sleepjob.id)

        # wait for our job to complete
        print "\n...waiting for job...\n"
        sleebjob_clone.wait()

        print "Job State   : %s" % (sleebjob_clone.state)
        print "Exitcode    : %s" % (sleebjob_clone.exit_code)
        print "Exec. hosts : %s" % (sleebjob_clone.execution_hosts)
        print "Create time : %s" % (sleebjob_clone.created)
        print "Start time  : %s" % (sleebjob_clone.started)
        print "End time    : %s" % (sleebjob_clone.finished)

    except saga.SagaException, ex:
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print " \n*** Backtrace:\n %s" % ex.traceback
        sys.exit(-1)

if __name__ == "__main__":
    main()
