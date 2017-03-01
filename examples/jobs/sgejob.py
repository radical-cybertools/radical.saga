#!/usr/bin/env python

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This examples shows how to run a job on a remote SGE cluster
    using the 'SGE' job adaptor.

    More information about the saga-python job API can be found at:
    http://saga-project.github.com/saga-python/doc/library/job/index.html
"""

import sys
import saga


def main():

    try:
        # Your ssh identity on the remote machine.
        ctx = saga.Context("ssh")

        # Change e.g., if you have a differnent username on the remote machine
        #ctx.user_id = "your_ssh_username"

        session = saga.Session()
        session.add_context(ctx)

        # Create a job service object that represent a remote pbs cluster.
        # The keyword 'pbs' in the url scheme triggers the SGE adaptors
        # and '+ssh' enables SGE remote access via SSH.
        js = saga.job.Service("sge+ssh://login1.ls4.tacc.utexas.edu",
                              session=session)

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = saga.job.Description()
        jd.environment       = {'FILENAME': 'testfile'}
        jd.wall_time_limit   = 1 # minutes
        
        jd.executable        = '/bin/touch'
        jd.arguments         = ['$FILENAME']

        jd.total_cpu_count   = 12 # for lonestar this has to be a multiple of 12
        jd.spmd_variation    = '12way' # translates to the qsub -pe flag
        #jd.total_physical_memory = 1024 # Memory requirements in Megabyte

        jd.queue             = "development"
        jd.project           = "TG-SEE100004"

        jd.working_directory = "$SCRATCH/A/B/C"
        jd.output            = "examplejob.out"
        jd.error             = "examplejob.err"

        # Create a new job from the job description. The initial state of 
        # the job is 'New'.
        touchjob = js.create_job(jd)

        # Check our job's id and state
        print "Job ID    : %s" % (touchjob.id)
        print "Job State : %s" % (touchjob.state)

        # Now we can start our job.
        print "\n...starting job...\n"
        touchjob.run()

        print "Job ID    : %s" % (touchjob.id)
        print "Job State : %s" % (touchjob.state)

        # List all jobs that are known by the adaptor.
        # This should show our job as well.
        print "\nListing active jobs: "
        for job in js.list():
            print " * %s" % job

        # wait for our job to complete
        print "\n...waiting for job...\n"
        touchjob.wait()

        print "Job State   : %s" % (touchjob.state)
        print "Exitcode    : %s" % (touchjob.exit_code)
        print "Exec. hosts : %s" % (touchjob.execution_hosts)
        print "Create time : %s" % (touchjob.created)
        print "Start time  : %s" % (touchjob.started)
        print "End time    : %s" % (touchjob.finished)

        js.close()
        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
