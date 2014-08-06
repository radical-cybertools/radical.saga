
import os
import sys
import saga

import saga.utils.benchmark as sb


# ------------------------------------------------------------------------------
#
def benchmark_pre (test_cfg, bench_cfg, session) :

    if  not 'job_service_url' in test_cfg :
        saga.NoSuccess ('no job service URL configured')

    return {'host'    : test_cfg['job_service_url'], 
            'session' : session}



# ------------------------------------------------------------------------------
#
def benchmark_core (args={}) :

    host    = args['host']
    session = args['session']

    js = saga.job.Service (host, session=session) 
    js.close ()


# ------------------------------------------------------------------------------
#
def benchmark_post (args={}) :

    pass


# ------------------------------------------------------------------------------
#
# services = []
# jobs     = []
# 
# for n in range (0, 100) :
#     services.append (saga.job.Service ('ssh://gw68.quarry.iu.teragrid.org'))
#     print n
# 
# print services
# 
# for js in services :
#     j = js.run_job ('/bin/sleep 100')
#     print j
#     jobs.append (j)
# 
# print jobs
# 
# import sys
# sys.exit (0)

try:
    sb.benchmark_init ('job_Servicec_create', benchmark_pre, benchmark_core, benchmark_post)

except saga.SagaException, ex:
    print "An exception occured: (%s) %s " % (ex.type, ex)
    print " \n*** Backtrace:\n %s" % ex.traceback




