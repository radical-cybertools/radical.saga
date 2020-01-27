#!/usr/bin/env python

__author__    = 'RADICAL @ Rutgers'
__copyright__ = 'Copyright 2012-2013, The SAGA Project'
__license__   = 'MIT'


'''
This examples shows how to run groups of jobs using the 'local' file adaptor.
This example uses job containers for simplified and optimized bulk job handling.

Job container can be used to model dependencies between groups of different
jobs, e.g., in workflow scenarios. In this example, we execute 'num_job_groups'
containers of jobs_per_group' number of parallel jobs sequentially::

    C1[j1,j2,j3,j4,...] -> C2[j1,j2,j3,j4,...] -> C3[j1,j2,j3,j4,...] -> ...

Depending on the adaptor implementation, using job containers can be quite
advantageous in terms of call latency. Some adaptors implement special bulk
operations for container management, which makes them generally much faster than
iterating over and operating on individual jobs.
'''

import sys
import random

import radical.saga as rs


URL = 'slurm://locahost/'


# ------------------------------------------------------------------------------
def main():

    # number of job 'groups' (containers) and of jobs per group
    num_job_groups = 10
    jobs_per_group =  2  # check slurm limits!

    current = None

    try:
        # all jobs in this example are running on the same job service
        service = rs.job.Service(URL)

        # create and populate our containers
        containers = list()
        for c in range(0, num_job_groups):

            # create containers
            containers.append(rs.job.Container())

            # add jobs to container.
            for j in range(0, jobs_per_group):
                jd = rs.job.Description()
                jd.executable  = '/bin/sleep'
                jd.arguments   = ['10']
                jd.name        = ['job.%02d.%03d' % (c, j)]
                j = service.create_job(jd)
                containers[c].add(j)

        # execute the containers sequentially
        for c in range(0, num_job_groups):
            print('run container %s ' % c)
            containers[c].run()

            current = containers[c]  # see exception handling

            for j in containers[c].get_tasks():
                print('%s: %s: %s' % (j.name, j.id, j.state))

            print(containers[c].get_states ())
            containers[c].wait()
            print(containers[c].get_states ())

            # all jobs in the container finished - print some job infos
            for job in containers[c].jobs:
                print('  * %s: %s (%s) @%s %s - %s' \
                  % (job.id, job.state, job.exit_code, job.execution_hosts,
                     job.started, job.finished))

            print()

        service.close()
        return 0

    except rs.SagaException as ex:

        print('An exception occured: %s ' % ((str(ex))))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(' *** %s' % ex.traceback)
        return -1


    finally:

        # make sure we leave no jobs behind
        if current is not None:
            print('\ncancel current container: %s' % current)
            current.cancel()
            for job in current.jobs:
                print('  * %s: %s (%s) @%s %s - %s' \
                  % (job.id, job.state, job.exit_code, job.execution_hosts,
                     job.started, job.finished))
            print()


# ------------------------------------------------------------------------------
if __name__ == '__main__':

    sys.exit(main())


# ------------------------------------------------------------------------------

