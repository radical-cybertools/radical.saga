""" This examples shows how to run a job on LoadLeveler cluster.

    More information about the saga-python job API can be found at:
    http://saga-project.github.com/saga-python/doc/library/job/index.html
"""

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import sys
import saga

CLUSTER_HOST = "your.loadleveler.cluster.hostname"
CLUSTER_NAME = "your.loadleveler.cluster.name"

# ----------------------------------------------------------------------------
#
def main():

    try:
        # Your ssh identity on the remote machine
        ctx = saga.Context("ssh")
        ctx.user_id = "your_username"

        session = saga.Session()
        session.add_context(ctx)

        # Create a job service object that represent a remote loadleveler
        # cluster. The keyword 'loadl' in the url scheme triggers the
        # LoadLeveler adaptors and '+ssh' enables LoadLeveler remote access
        # via SSH. and 'cluster' URL query specify loadleveler cluster name.
        # "llq -X cluster"
        js = saga.job.Service("loadl+ssh://%s?cluster=%s" % \
            (CLUSTER_HOST, CLUSTER_NAME), session=session)

        # describe our job
        jd = saga.job.Description()

        # Next, we describe the job we want to run. A complete set of job
        # description attributes can be found in the API documentation.
        jd.environment     = {'MYOUTPUT':'"Hello LoadLevler Adaptor from SAGA"'}
        jd.executable      = '/bin/echo'
        jd.arguments       = ['$MYOUTPUT']
        jd.output          = "/tmp/mysagajob.stdout"
        jd.error           = "/tmp/mysagajob.stderr"

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

        print "Job ID    : %s" % (myjob.id)
        print "Job State : %s" % (myjob.state)
        print "Exitcode  : %s" % (myjob.exit_code)

        js.close()
        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occurred: (%s) %s " % (ex.type, (str(ex)))
        # Get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
