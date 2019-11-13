#!/usr/bin/env python

""" This examples shows how to run a job on a remote TORQUE cluster
    using the 'PBS' job adaptor via GSISSH.

    More information about the radical.saga job API can be found at:
    http://radical-cybertools.github.com/radical.saga/doc/library/job/index.html
"""

import sys

import radical.saga as rs


# ----------------------------------------------------------------------------
# This is an example for a callback function. Callback functions can be
# registered with a rs.Job object and get 'fired' asynchronously on
# certain conditions.
def job_state_change_cb(src_obj, fire_on, value):
    print("Callback    : job state changed to '%s'\n" % value)
    return True


# ----------------------------------------------------------------------------
#
def main():

    try:
        session = rs.Session()

        # Create a job service object that represent a remote pbs cluster.
        # The keyword 'pbs' in the url scheme triggers the PBS adaptors
        # and '+ssh' enables PBS remote access via SSH.
        js = rs.job.Service("torque+gsissh://supermic.cct-lsu.xsede.org:2222",
                              session=session)

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = rs.job.Description()
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
        job = js.create_job(jd)

        # Register our callback. We want it to 'fire' on job state change
        job.add_callback(rs.STATE, job_state_change_cb)

        # Check our job's id and state
        print("Job ID      : %s" % (job.id))
        print("Job State   : %s" % (job.state))

        # Now we can start our job.
        print("\n...starting job...\n")
        job.run()

        print("Job ID      : %s" % (job.id))

        # List all jobs that are known by the adaptor.
        # This should show our job as well.
        print("\nListing active jobs: ")
        for job in js.list():
            print(" * %s" % job)

        # wait for our job to complete
        print("\n...waiting for job...\n")
        job.wait()

        print("Job State   : %s" % job.state)
        print("Exitcode    : %s" % job.exit_code)
        print("Exec. hosts : %s" % job.execution_hosts)
        print("Create time : %d" % job.created)
        print("Start time  : %d" % job.started)
        print("End time    : %d" % job.finished)

        js.close()
        return 0

    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(main())

# ------------------------------------------------------------------------------

