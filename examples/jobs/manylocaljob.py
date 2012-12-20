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

    num_services = 1
    num_jobs     = 10
    job_runtime  = 10 # seconds
    
    try:
        jd = saga.job.Description()

        # environment, executable & arguments
        jd.environment = {'RUNTIME':str(job_runtime)}       
        jd.executable  = '/bin/sleep'
        jd.arguments   = ['$RUNTIME']

        # create job service(s)
        services = list()
        for s in range(0, num_services):
            services.append(saga.job.Service("fork://localhost"))
            print "New job service: %s" % s

        # launch 'num_jobs' per service
        for service in services:
            for j in range(0, num_jobs):
                job = service.create_job(jd)
                job.run()

        tc = saga.job.Container()

        for service in services:
            print "Serivce: %s" % service  
            for job_id in service.list():
                job = service.get_job(job_id)
                tc.add(job)
                print " * ID: %s State: %s RC: %s" % (job.id, job.state, job.exit_code)
                job.cancel()

        tc.wait()



        #import time
        #time.sleep(11)


        for service in services:
            print "Serivce: %s" % service  
            for job_id in service.list():
                job = service.get_job(job_id)
                print " * ID: %s State: %s RC: %s" % (job.id, job.state, job.exit_code)
        


    except saga.SagaException, ex:
        print "An exception occured: %s " % ((str(ex)))
        # get the whole traceback - this might be helpful
        print " *** %s" % saga.utils.exception.get_traceback()
        sys.exit(-1)

if __name__ == "__main__":
    main()
