#!/usr/bin/env python

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import saga

def main():
    try:
        # Create a job service object that represent the local machine.
        # The keyword 'fork://' in the url scheme triggers the 'shell' adaptor
        # which can execute jobs on the local machine as well as on a remote
        # machine via "ssh://hostname".
        js = saga.job.Service("fork://localhost")

        # describe our job
        jd = saga.job.Description()

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}
        jd.executable      = '/bin/echo'
        jd.arguments       = ['$MYOUTPUT']
        jd.output          = "mysagajob.stdout"
        jd.error           = "mysagajob.stderr"

        # Create a new job from the job description. The initial state of
        # the job is 'New'.
        myjob = js.create_job(jd)

        # Check our job's id and state
        print "Job ID    : %s" % (myjob.id)
        print "Job State : %s" % (myjob.state)

        print "\n...starting job...\n"

        # Now we can start our job.
        myjob.run()

        print "Job ID    : %s" % (myjob.id)
        print "Job State : %s" % (myjob.state)

        print "\n...waiting for job...\n"
        # wait for the job to either finish or fail
        myjob.wait()

        print "Job State : %s" % (myjob.state)
        print "Exitcode  : %s" % (myjob.exit_code)

        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())
