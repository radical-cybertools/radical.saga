

import saga.utils.benchmark as sb

import os
import sys
import saga


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, test_cfg, bench_cfg, session) :

    if  not 'job_service_url' in test_cfg :
        raise saga.NoSuccess ('no job service URL configured')

    if  not 'load' in bench_cfg : 
        raise saga.NoSuccess ('no benchmark load configured')

    host = test_cfg['job_service_url']
    n_j  = int(bench_cfg['iterations'])  
    load = int(bench_cfg['load'])       

    js = saga.job.Service (host, session=session) 
    jd = saga.job.Description ()

    jd.executable = '/bin/sleep'
    jd.arguments  = ['1']

    return {'js'   : js   , 
            'jd'   : jd   , 
            'load' : load }


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, args={}) :

    js        = args['js']
    jd        = args['jd']
    load      = args['load']

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
def benchmark_post (tid, args={}) :

    pass


# ------------------------------------------------------------------------------
#
try:

    sb.benchmark_init ('job_run_bulk', benchmark_pre, benchmark_core, benchmark_post)

except saga.SagaException, ex:
    print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
    print " \n*** Backtrace:\n %s" % ex.traceback




