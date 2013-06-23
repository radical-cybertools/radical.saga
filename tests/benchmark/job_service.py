
import os
import sys
import saga

import saga.utils.misc as sumisc

try:

    # get test backend and benchmark configurations
    (test_cfg, bench_cfg, session) = sumisc.benchmark_init ()

    if  not 'job_service_url' in test_cfg :
        sumisc.benchmark_eval ('no job service URL configured')

    if  not 'n_js' in bench_cfg :
        sumisc.benchmark_eval ('no job service count configured')

    if  not 'n_j' in bench_cfg :
        sumisc.benchmark_eval ('no job count configured')

    if  not 'n_j' in bench_cfg :
        TIME = 10
    else :
        TIME = int(bench_cfg['sleep'])

    N_JS = int(bench_cfg['n_js'])
    N_J  = int(bench_cfg['n_j'])
    HOST = test_cfg['job_service_url']


    sumisc.benchmark_start (HOST, 'job.Service startup')

    for i in range (-1, N_JS):
        ctx = saga.Context("ssh")
        ctx.user_id = USER_ID

        session = saga.Session()
        session.add_context(ctx)

        js = saga.job.Service ("%s" % HOST, session=session) 
        jd = saga.job.Description()

        jd.executable          = '/bin/sleep'
        jd.queue               = 'normal'
        jd.project             = 'TG-MCB090174'
        jd.wall_time_limit     = TIME + 1 # should be positive
        jd.total_cpu_count     = 1
        jd.number_of_processes = 1
        jd.arguments           = [TIME]
        jd.output              = "/tmp/saga_job.%s.stdout" % USER_ID
        jd.error               = "/tmp/saga_job.%s.stderr" % USER_ID

        # js.close ()
        
        sumisc.benchmark_tic ()

    sumisc.benchmark_eval ()
    sys.exit (0)

except saga.SagaException, ex:
    print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
    print " \n*** Backtrace:\n %s" % ex.traceback
    sys.exit (-1)

