
import radical.utils.benchmark as rb

import os
import sys
import saga


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, app_cfg, bench_cfg) :

    if  not 'saga.tests' in app_cfg :
        raise saga.NoSuccess ('no tests configured')

    if  not 'job_service_url' in app_cfg['saga.tests'] :
        raise saga.NoSuccess ('no job service URL configured')

    if  not 'load' in bench_cfg :
        raise saga.NoSuccess ('no test load configured')

    host = str(app_cfg['saga.tests']['job_service_url'])
    load = int(bench_cfg['load'])

    js = saga.job.Service (host) 
    jd = saga.job.Description()

    jd.executable = '/bin/sleep'
    jd.arguments  = [load]

    app_cfg['js'] = js
    app_cfg['jd'] = jd


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, app_cfg, bench_cfg) :

    js = app_cfg['js']
    jd = app_cfg['jd']

    j  = js.create_job (jd)
    j.run()


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
b = rb.Benchmark (sys.argv[1], 'job_run', benchmark_pre, benchmark_core, benchmark_post)
b.run  ()
b.eval ()


