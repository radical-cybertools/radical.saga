#!/usr/bin/env python

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This examples shows how to run a job on a remote SLURM cluster
    using the 'SLURM' job adaptor.

    More information about the radical.saga job API can be found at:
    http://radical-cybertools.github.com/radical.saga/doc/library/job/index.html
"""

import sys

import radical.saga as rs


js_url = "slurm://localhost/"


# ------------------------------------------------------------------------------
#
def start():

    try:
        # Create a job service object that represent a remote pbs cluster.
        # The keyword 'pbs' in the url scheme triggers the SGE adaptors
        # and '+ssh' enables SGE remote access via SSH.
        js = rs.job.Service(js_url)

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = rs.job.Description()
        jd.environment       = {'FILENAME': 'testfile'}
        jd.wall_time_limit   = 1  # minutes

        jd.executable        = '/bin/touch'
        jd.arguments         = ['$FILENAME']

        jd.name              = "examplejob"
      # jd.queue             = "normal"
      # jd.project           = "TG-MCB090174" 

        jd.working_directory = ".saga/test"
        jd.output            = "examplejob.out"
        jd.error             = "examplejob.err"

        # Create a new job from the job description. The initial state of
        # the job is 'New'.
        job = js.create_job(jd)

        # Check our job's id and state
        print("Job State   : %s" % (job.state))

        # Now we can start our job.
        print("starting job")
        job.run()

        print("Job ID      : %s" % (job.id))
        print("Job State   : %s" % job.state)
        print("Exitcode    : %s" % job.exit_code)
        print("Exec. hosts : %s" % job.execution_hosts)
        print("Create time : %s" % job.created)
        print("Start time  : %s" % job.started)
        print("End time    : %s" % job.finished)

        js.close()

    except rs.SagaException as e:

        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (e.type, (str(e))))

        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" \n*** Backtrace:\n %s" % e.traceback)
        return -1

# ------------------------------------------------------------------------------
#
def check(jobid):

    try:
        # Create a job service object to the same cluster
        js  = rs.job.Service(js_url)

        # List all jobs that are known by the adaptor.
        # This should show our job as well.
        print("Listing active jobs: ")
        for jid in js.list():
            if jid == jobid:
                print(' * %s' % jid)
            else:
                print(' - %s' % jid)

        # reconnect to the given job
        job = js.get_job(jobid)

        print("Job State   : %s" % job.state)
        print("Exitcode    : %s" % job.exit_code)
        print("Exec. hosts : %s" % job.execution_hosts)
        print("Create time : %s" % job.created)
        print("Start time  : %s" % job.started)
        print("End time    : %s" % job.finished)

        js.close()

    except rs.SagaException as e:

        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (e.type, (str(e))))

        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" \n*** Backtrace:\n %s" % e.traceback)
        return -1

# ------------------------------------------------------------------------------
#
def stop(jobid):

    try:

        # Create a job service object to the same cluster and reconnect to job
        js  = rs.job.Service(js_url)
        job = js.get_job(jobid)
        print("Job ID    : %s" % (job.id))
        print("Job State : %s" % (job.state))

        print("cacnel job")
        job.cancel()

        # wait for our job to complete
        print("wait for job")
        job.wait()

        print("Job State   : %s" % job.state)
        print("Exitcode    : %s" % job.exit_code)
        print("Exec. hosts : %s" % job.execution_hosts)
        print("Create time : %s" % job.created)
        print("Start time  : %s" % job.started)
        print("End time    : %s" % job.finished)

        js.close()
        return 0

    except rs.SagaException as e:

        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (e.type, (str(e))))

        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" \n*** Backtrace:\n %s" % e.traceback)
        return -1

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("\n\tusage: %s [start | check | stop] <jobid>\n" % sys.argv[0])
        sys.exit(-1)

    if   sys.argv[1] == 'start': sys.exit(start())
    elif sys.argv[1] == 'check': sys.exit(check(sys.argv[2]))
    elif sys.argv[1] == 'stop' : sys.exit(stop(sys.argv[2]))

