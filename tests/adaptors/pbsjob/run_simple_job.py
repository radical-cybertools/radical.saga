
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" TODO: Missing Docstring
"""

import sys
import radical.saga as saga


def main():

    try:
        # create a job service for the local machine. both, 'fork' and
        # 'local' schemes trigger the local job adaptor.
        js = saga.job.Service("pbs://localhost")

        # describe our job
        jd = saga.job.Description()

        # environment, executable & arguments. We use '/bin/sleep' to simulate
        # a job that runs for $RUNTIME seconds.
        jd.environment = {'RUNTIME': '10'}
        jd.executable  = '/bin/sleep'
        jd.arguments   = ['$RUNTIME']

        # output options (will just be empty files for /bin/sleep)
        jd.output = "saga_localjob.stdout"
        jd.error  = "saga_localjob.stderr"

        # create the job (state: New)
        catjob = js.create_job(jd)

        # check our job's id and state
        print("Job ID    : %s" % (catjob.id))
        print("Job State : %s" % (catjob.state))

        print("\n...starting job...\n")
        catjob.run()

        print("Job ID    : %s" % (catjob.id))
        print("Job State : %s" % (catjob.state))

        print("\nListing active jobs: ")
        for job in js.list():
            print(" * %s" % job)

        # wait for our job to complete
        print("\n...waiting for job...\n")
        catjob.wait()

        print("Job State : %s" % (catjob.state))
        print("Exitcode  : %s" % (catjob.exit_code))

    except saga.SagaException as ex:
        print("An exception occured: %s " % ((str(ex))))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" *** %s" % ex.traceback)
        sys.exit(-1)

if __name__ == "__main__":
    main()
