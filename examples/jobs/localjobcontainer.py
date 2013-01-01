#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

""" This examples shows how to run groups of jobs using the 
    'local' file adaptor. This example uses job containers for 
    simplified and optimized bulk job handling.

    Job container can be used to easily model dependencies between 
    groups of different jobs, e.g., in workflow scenarios. In this example, 
    we execute 'num_job_groups' containers of jobs_per_group' number of 
    parallel jobs sequentially::
    
      C1[j1,j2,j3,j4,...] -> C2[j1,j2,j3,j4,...] -> C3[j1,j2,j3,j4,...] -> ... 

    Depending on the adaptor implementation, using job containers can be 
    quite advantageous in terms of call latency. Some adaptors implement 
    special bulk operations for container management, which makes them 
    generally much faster than iterating over and operating on individual jobs.
"""

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, The SAGA Project"
__license__   = "MIT"

import sys, random, saga

def main():

    # number of job 'groups' / containers
    num_job_groups = 1
    # number of jobs per container
    jobs_per_group = 1
    
    try:
        # all jobs in this example are running on the same job service
        # this is not a requirement though. s
        service = saga.job.Service("fork://localhost")
        print service.url

        # create and populate our containers
        containers = list()
        for c in range(0, num_job_groups):
            # create containers
            containers.append(saga.job.Container())
            for j in range(0, jobs_per_group):
                # add jobs to container. to make things a bit more 
                # interesting, we give each job a random runtime (1-60s)
                jd = saga.job.Description()
              # jd.environment = {'RUNTIME': random.randrange(1,60,1)}       
                jd.executable  = '/bin/sleep'
                jd.arguments   = ['$RUNTIME']
                containers[c].add(service.create_job(jd))

        # execute the containers sequentially
        for c in range(0, num_job_groups):
            print 'Starting container %s ... ' % c
            containers[c].run()
            containers[c].wait()
            
            # print containers[c].get_states ()

            # at this point, all jobs in the container
            # have finished running. we can now print some statistics 
            for job in containers[c].jobs:
                print "  * Job id=%s state=%s rc=%s exec_host=%s start_time=%s end_time=%s" \
                  % (job.id, job.state, job.exit_code, job.execution_hosts, job.started, job.finished)


    except saga.SagaException, ex:
        print "An exception occured: %s " % ((str(ex)))
        # get the whole traceback - this might be helpful
        print " *** %s" % saga.utils.exception.get_traceback()
        sys.exit(-1)

if __name__ == "__main__":
    main()

