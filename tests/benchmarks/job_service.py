
import os
import sys
import saga

import radical.utils.benchmark as rb


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, app_cfg, bench_cfg) :

    if  not 'saga.tests' in app_cfg :
        raise saga.NoSuccess ('no tests configured')

    if  not 'job_service_url' in app_cfg['saga.tests'] :
        raise saga.NoSuccess ('no job service URL configured')

    host = str(app_cfg['saga.tests']['job_service_url'])

    app_cfg['host'] = host


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, app_cfg, bench_cfg) :

    host = app_cfg['host']

    js = saga.job.Service (host) 
    js.close ()


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
b = rb.Benchmark (sys.argv[1], 'job_service_create', benchmark_pre, benchmark_core, benchmark_post)
b.run  ()
b.eval ()

