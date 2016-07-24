
from __future__ import absolute_import
from __future__ import print_function
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


""" Test for https://github.com/saga-project/saga-python/issues/133
"""

import os
import sys
import saga

print(os.environ['PS1'])


ORIG_PS1 = os.environ['PS1']
os.environ['PS1']='\[\033[1;37m\][\[\033[1;32m\]\u\[\033[0m\]@\h\[\033[0m\] \[\033[33;1m\]\w\[\033[36m\]$(__git_ps1 " %s")\[\033[1;37m\]]\[\033[0m\] '

print(os.environ['PS1'])

def main():

    try:

        # set PS1



        # Create a job service object that represent the local machine.
        # The keyword 'fork' in the url scheme triggers the 'shell' adaptor.
        # The adaptor also support ssh:// and gsissh://
        js = saga.job.Service("fork://localhost")

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = saga.job.Description()

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd = saga.job.Description()
        jd.environment       = {'FILENAME': 'testfile'}
        jd.wall_time_limit   = 1 # minutes

        jd.executable        = '/usr/bin/touch'
        jd.arguments         = ['$FILENAME']

        jd.working_directory = "$HOME/A/B/C"
        jd.output            = "examplejob.out"
        jd.error             = "examplejob.err"

        # Create a new job from the job description. The initial state of
        # the job is 'New'.
        touchjob = js.create_job(jd)

        # Check our job's id and state
        print("Job ID    : %s" % (touchjob.id))
        print("Job State : %s" % (touchjob.state))

        # Now we can start our job.
        print("\n...starting job...\n")
        touchjob.run()

        print("Job ID    : %s" % (touchjob.id))
        print("Job State : %s" % (touchjob.state))

        # List all jobs that are known by the adaptor.
        # This should show our job as well.
        print("\nListing active jobs: ")
        for job in js.list():
            print(" * %s" % job)

        # Now we disconnect and reconnect to our job by using the get_job()
        # method and our job's id. While this doesn't make a lot of sense
        # here,  disconnect / reconnect can become very important for
        # long-running job.
        touchjob_clone = js.get_job(touchjob.id)

        # wait for our job to complete
        print("\n...waiting for job...\n")
        touchjob_clone.wait()

        print("Job State   : %s" % (touchjob_clone.state))
        print("Exitcode    : %s" % (touchjob_clone.exit_code))
        print("Exec. hosts : %s" % (touchjob_clone.execution_hosts))
        print("Create time : %s" % (touchjob_clone.created))
        print("Start time  : %s" % (touchjob_clone.started))
        print("End time    : %s" % (touchjob_clone.finished))

        return 0

    except saga.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1

    finally:
        os.environ['PS1'] = ORIG_PS1


if __name__ == "__main__":
    sys.exit(main())
