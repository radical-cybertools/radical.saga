

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
    jd = saga.job.Description ()

    jd.executable = '/bin/sleep'
    jd.arguments  = ['1']

    app_cfg['js'  ] = js
    app_cfg['jd'  ] = jd
    app_cfg['load'] = load


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, app_cfg, bench_cfg) :

    js   = app_cfg['js']
    jd   = app_cfg['jd']
    load = app_cfg['load']

    tc = saga.task.Container  ()
  # jd = saga.job.Description ()

    for n in range (0, load) :

  #     jd.executable = '/bin/touch'
  #     jd.arguments  = ["/tmp/touch_test_%05d_%05d_%05d.dat" % (tid, i, n)]

        j  = js.create_job (jd)
        tc.add (j)

    tc.run ()


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
b = rb.Benchmark (sys.argv[1], 'job_run_bulk', benchmark_pre, benchmark_core, benchmark_post)
b.run  ()
b.eval ()




