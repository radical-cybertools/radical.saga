#!/usr/bin/env python

""" This examples shows how to run a job on a remote TORQUE cluster
    using the 'PBS' job adaptor via GSISSH.

    More information about the saga-python job API can be found at:
    http://saga-project.github.com/saga-python/doc/library/job/index.html
"""

import sys
import saga


# ----------------------------------------------------------------------------
# This is an example for a callback function. Callback functions can be
# registered with a saga.Job object and get 'fired' asynchronously on
# certain conditions.
def job_state_change_cb(src_obj, fire_on, value):
    print "Callback    : job state changed to '%s'\n" % value
    return True


# ----------------------------------------------------------------------------
#
def main():

    try:
        session = saga.Session()

        # Create a job service object that represent a remote pbs cluster.
        # The keyword 'pbs' in the url scheme triggers the PBS adaptors
        # and '+ssh' enables PBS remote access via SSH.
        js = saga.job.Service("torque+gsissh://supermic.cct-lsu.xsede.org:2222",
                              session=session)

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = saga.job.Description()
        jd.environment       = {'FILENAME': 'testfile'}
        jd.wall_time_limit   = 1  # minutes
        
        jd.executable        = '/bin/touch'
        jd.arguments         = ['$FILENAME']

        jd.total_cpu_count   = 20

        jd.queue             = "workq"
      # jd.project           = "TG-MCB090174"

        jd.working_directory = "$HOME/A/B/C"
        jd.output            = "examplejob.out"
        jd.error             = "examplejob.err"

        # Create a new job from the job description. The initial state of 
        # the job is 'New'.
        touchjob = js.create_job(jd)

        # Register our callback. We want it to 'fire' on job state change
        touchjob.add_callback(saga.STATE, job_state_change_cb)

        # Check our job's id and state
        print "Job ID      : %s" % (touchjob.id)
        print "Job State   : %s" % (touchjob.state)

        # Now we can start our job.
        print "\n...starting job...\n"
        touchjob.run()

        print "Job ID      : %s" % (touchjob.id)

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
        print "Create time : %d" % (touchjob.created)
        print "Start time  : %d" % (touchjob.started)
        print "End time    : %d" % (touchjob.finished)

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
