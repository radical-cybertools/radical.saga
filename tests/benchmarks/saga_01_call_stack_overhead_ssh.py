
import saga.utils.benchmark as sb

import os
import sys
import time
import saga


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, test_cfg, bench_cfg, session) :

    if  not 'job_service_url' in test_cfg :
        raise saga.NoSuccess ('no job service URL configured')

    if  not 'load' in bench_cfg :
        raise saga.NoSuccess ('no benchmark load configured')

    host  = saga.Url(test_cfg['job_service_url']).host
    n_j   = int(bench_cfg['iterations'])
    load  = int(bench_cfg['load'])
    exe   = '/bin/sleep'

    ssh   = subprocess ("ssh %s" % host, stdin  = subprocess.PIPE, 
                                         stdout = subprocess.PIPE,
                                         stderr = subprocess.STDOUT)

    # find the ssh prompt
    stdin  = ssh.communicate[0]
    stdout = ssh.communicate[1]

    while <stdin> ~! /'>$'/io :
        time.sleep (0.1)

    # setup is done
    return {'ssh' : ssh, 'cmd' : "%s %s" % (executable, load)}


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, args={}) :

    ssh = args['ssh']
    exe = args['exe']

    stdin  = ssh.stdin
    stdout = ssh.stdout
    stderr = ssh.stderr

    j.run()


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, args={}) :

    pass


# ------------------------------------------------------------------------------
#
try:

    sb.benchmark_init ('job_run', benchmark_pre, benchmark_core, benchmark_post)

except saga.SagaException, ex:
    print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
    print " \n*** Backtrace:\n %s" % ex.traceback




