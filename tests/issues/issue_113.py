
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" https://github.com/saga-project/saga-python/issues/113
"""

import sys
import radical.saga as saga
import time

ITERATIONS=32

def main():

    try:
        js = saga.job.Service("pbs://localhost")

        jd = saga.job.Description()
        jd.environment     = {'RUNTIME': '120'}
        jd.wall_time_limit = 4 # minutes
        jd.executable      = '/bin/sleep'
        jd.arguments       = ['$RUNTIME']

        for step in range(0, ITERATIONS):
            sleepjob = js.create_job(jd)

            # Check our job's id and state
            print "Job ID    : %s" % (sleepjob.id)
            print "Job State : %s" % (sleepjob.state)

            # Now we can start our job.
            print "\n...starting job...\n"
            sleepjob.run()

            while sleepjob.state != saga.job.RUNNING:
                time.sleep(2) # wait two seconds

            print "\n...canceling job....\n"
            sleepjob.cancel()

            print "Job State : %s" % (sleepjob.state)

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
