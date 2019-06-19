#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


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

import sys
import random
import time

import radical.saga as rs


URL = "condor+gsissh://xd-login.opensciencegrid.org"
URL = "fork://locahost/"

def main():

    # number of job 'groups' / containers
    num_job_groups = 2
    # number of jobs per container
    jobs_per_group = 10

    try:
        # all jobs in this example are running on the same job service
        # this is not a requirement though. s
        service = rs.job.Service(URL)
        print(service.url)

        t1 = time.time()
        # create and populate our containers
        containers = list()
        for c in range(0, num_job_groups):
            # create containers
            containers.append(rs.job.Container())
            for j in range(0, jobs_per_group):
                # add jobs to container. to make things a bit more
                # interesting, we give each job a random runtime (1-60s)
                jd = rs.job.Description()
                jd.environment = {'RUNTIME': random.randrange(10, 60)}
                jd.executable  = '/bin/sleep'
                jd.arguments   = ['$RUNTIME']
                jd.name        = ['job.%02d.%03d' % (c, j)]
                jd.project         = 'TG-CCR140028'
                j = service.create_job(jd)
                containers[c].add(j)

        # execute the containers sequentially
        for c in range(0, num_job_groups):
            print('Starting container %s ... ' % c)
            containers[c].run()

            for j in containers[c].get_tasks():
                print('%s: %s: %s' % (j.name, j.id, j.state))

            print(containers[c].get_states ())
            containers[c].cancel()
            containers[c].wait()
            print(containers[c].get_states ())

            # # at this point, all jobs in the container
            # # have finished running. we can now print some statistics
            # for job in containers[c].jobs:
            #     print "  * Job id=%s state=%s rc=%s exec_host=%s start_time=%s end_time=%s" \
            #       % (job.id, job.state, job.exit_code, job.execution_hosts,
            #          job.started, job.finished)
        t2 = time.time()
        print(t2-t1)

        service.close()
        return 0

    except rs.SagaException as ex:
        print("An exception occured: %s " % ((str(ex))))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" *** %s" % ex.traceback)
        return -1

if __name__ == "__main__":
    sys.exit(main())
