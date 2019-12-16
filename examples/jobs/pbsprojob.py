
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This examples shows how to run a job on a remote PBSPRO cluster
    using the 'PBSPRO' job adaptor.

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
        # Your ssh identity on the remote machine.
        ctx = rs.Context("ssh")

        # Change e.g., if you have a differnent username on the remote machine
        #ctx.user_id = "your_ssh_username"

        session = rs.Session()
        session.add_context(ctx)

        # Create a job service object that represent a remote pbs cluster.
        # The keyword 'pbs' in the url scheme triggers the PBS adaptors
        # and '+ssh' enables PBS remote access via SSH.
        js = rs.job.Service("pbspro://localhost/",
                              session=session)

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = rs.job.Description()
        jd.wall_time_limit   = 1 # minutes
        jd.executable        = '/bin/data'

       #jd.total_cpu_count   = 12 # for lonestar this has to be a multiple of 12
       #jd.spmd_variation    = '12way' # translates to the qsub -pe flag

       #jd.queue             = "batch"
        jd.project           = "e291"

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
        for jid in js.list():
            print(" * %s" % jid)

        # wait for our job to complete
        print("\n...waiting for job...\n")
        job.wait()

        print("Job State   : %s" % (job.state))
        print("Exitcode    : %s" % (job.exit_code))
        print("Exec. hosts : %s" % (job.execution_hosts))
        print("Create time : %s" % (job.created))
        print("Start time  : %s" % (job.started))
        print("End time    : %s" % (job.finished))

        js.close()
        return 0

    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1

if __name__ == "__main__":
    sys.exit(main())
