#!/usr/bin/env python
# encoding: utf-8

""" This examples shows how to run a job on a remote SGE cluster
    using the 'sgejob' job adaptor.
"""

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import sys
import saga


def main():

    try:
        # create a job service for a remote SGE cluster.
        # the 'sge+ssh://' scheme triggers the SGE adaptor.
        js = saga.job.Service("sge+ssh://lonestar.tacc.utexas.edu")

        # describe our job
        jd = saga.job.Description()

        # environment, executable & arguments. We use '/bin/sleep' to simulate
        # a job that runs for $RUNTIME seconds.
        jd.queue       = 'normal'
        jd.name        = 'testjob'
        jd.project     = 'TG-MCB090174'
        jd.environment = {'RUNTIME': '60'}
        jd.wall_time_limit = 2  # minutes
        jd.total_cpu_count = 1
        jd.working_directory = "/tmp/"
        jd.executable  = '/bin/sleep'
        jd.arguments   = ['$RUNTIME']

        # output options (will just be empty files for /bin/sleep)
        jd.output = "saga_sgejob.stdout"
        jd.error  = "saga_sgejob.stderr"

        # create the job (state: New)
        sleepjob = js.create_job(jd)

        # check our job's id and state
        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        print "\n...starting job...\n"
        sleepjob.run()

        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        #print "\nListing active jobs: "
        #for job in js.list():
        #    print " * %s" % job

        # wait for our job to complete
        print "\n...waiting for job...\n"
        sleepjob.wait(10)
        sleepjob.cancel()

        print "Job State   : %s" % (sleepjob.state)
        print "Exitcode    : %s" % (sleepjob.exit_code)
        print "Exec. hosts : %s" % (sleepjob.execution_hosts)
        print "Create time : %s" % (sleepjob.created)
        print "Start time  : %s" % (sleepjob.started)
        print "End time    : %s" % (sleepjob.finished)

    except saga.SagaException, ex:
        print "An exception occured: %s " % ((str(ex)))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print " *** %s" % ex.traceback
        sys.exit(-1)

if __name__ == "__main__":
    main()
