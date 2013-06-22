
import os
import sys
import saga

import saga.utils.misc as sumisc

# ------------------------------------------------------------------------------
#
def benchmark_pre (test_cfg, bench_cfg, session) :

    if  not 'job_service_url' in test_cfg :
        sumisc.benchmark_eval ('no job service URL configured')

    if  not 'load'        in bench_cfg : 
        sumisc.benchmark_eval ('no benchmark load configured')

    host = test_cfg['job_service_url']
    n_j  = int(bench_cfg['iterations'])  
    load = int(bench_cfg['load'])       

    js = saga.job.Service (host, session=session) 
    jd = saga.job.Description()

    jd.executable = '/bin/sleep'
    jd.arguments  = [load]

    return {'js' : js, 'jd' : jd}


# ------------------------------------------------------------------------------
#
def benchmark_core (args={}) :

    js = args['js']
    jd = args['jd']

    j  = js.create_job (jd)
    j.run()


# ------------------------------------------------------------------------------
#
def benchmark_post (args={}) :

    pass


# ------------------------------------------------------------------------------
#
try:

    sumisc.benchmark_init ('job.run', benchmark_pre, benchmark_core, benchmark_post)

except saga.SagaException, ex:
    print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
    print " \n*** Backtrace:\n %s" % ex.traceback


