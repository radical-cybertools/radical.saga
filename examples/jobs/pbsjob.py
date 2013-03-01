#!/usr/bin/env python
# encoding: utf-8

""" This examples shows how to run a job on a remote PBS/TORQUE cluster
    using the 'local' job adaptor.
"""

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import sys
import time
import saga


def main():

    try:
        # create a job service for the local machine. both, 'fork' and
        # 'local' schemes trigger the local job adaptor.
        js = saga.job.Service("pbs+ssh://alamo.futuregrid.orgs")

        # describe our job
        jd = saga.job.Description()

        # environment, executable & arguments. We use '/bin/sleep' to simulate
        # a job that runs for $RUNTIME seconds.
        jd.queue       = 'batch'
        jd.name        = 'testjob'
        jd.project     = 'TG-MCB090174'
        jd.environment = {'RUNTIME': '10'}
        jd.wall_time_limit =   2 # minutes
        #jd.total_cpu_count = 12
        jd.working_directory = "/tmp/"
        jd.executable  = '/bin/sleep'
        jd.arguments   = ['$RUNTIME']

        # output options (will just be empty files for /bin/sleep)
        jd.output = "saga_pbsjob.stdout"
        jd.error  = "saga_pbsjob.stderr"

        # create the job (state: New)
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
        print "An exception occured: (%s) %s " % (ex.get_type(), (str(ex)))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print " \n*** Backtrace:\n %s" % ex.traceback
        sys.exit(-1)

if __name__ == "__main__":
    main()
