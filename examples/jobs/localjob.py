
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This examples shows how to run a job on the local machine
    using the 'local' job adaptor.
"""

import sys
import saga


def main():

    try:
        # Create a job service object that represent the local machine.
        # The keyword 'fork' in the url scheme triggers the 'shell' adaptor.
        # The adaptor also support ssh:// and gsissh://
        js = saga.job.Service("fork://localhost")

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = saga.job.Description()

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd.environment     = {'RUNTIME': '10'}
        jd.wall_time_limit = 1 # minutes
        jd.executable      = '/bin/sleep'
        jd.arguments       = ['$RUNTIME']

        # Create a new job from the job description. The initial state of
        # the job is 'New'.
        sleepjob = js.create_job(jd)

        # Check our job's id and state
        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        # Now we can start our job.
        print "\n...starting job...\n"
        sleepjob.run()

        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        # List all jobs that are known by the adaptor.
        # This should show our job as well.
        print "\nListing active jobs: "
        for job in js.list():
            print " * %s" % job

        # Now we disconnect and reconnect to our job by using the get_job()
        # method and our job's id. While this doesn't make a lot of sense
        # here,  disconnect / reconnect can become very important for
        # long-running job.
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

        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
