#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

""" This examples shows how to execute a job to the local machine
    via the local job adaptor.
"""

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, The SAGA Project"
__license__   = "MIT"

import sys, saga

def main():
    
    try:
        # create a job service for the local machine
        js = saga.job.Service("fork://localhost")

        # describe our job
        jd = saga.job.Description()
        # resource requirements
        jd.total_cpu_count  = 1     
        # environment, executable & arguments
        jd.environment = {'CATME':'Hello from SAGA'}       
        jd.executable  = '/bin/cat'
        jd.arguments   = ['$CATME']
        
        # output options (will be just empty files for /bin/sleep)
        jd.output = "saga_localjob.stdout"
        jd.error  = "saga_localjob.stderr"

        # create the job (state: New)
        catjob = js.create_job(jd)

        print "Job ID    : %s" % (catjob.job_id)
        print "Job State : %s" % (catjob.state)

        print "\n...starting job...\n"
        catjob.run()

        print "Job ID    : %s" % (catjob.job_id)
        print "Job State : %s" % (catjob.state)

        print "\n...waiting for job...\n"
        # wait for the job to either finish or fail
        catjob.wait()

        print "Job State : %s" % (catjob.state)
        print "Exitcode  : %s" % (catjob.exitcode)

    except saga.SagaException, ex:
        print "An exception occured during job execution: %s (%s)" % ((str(ex)), ex.object )
        print ex.traceback
        sys.exit(-1)

if __name__ == "__main__":
    main()
