
import os
import sys
import saga

import saga.utils.misc as sumisc

USER_ID  = "merzky"
HOST     = "ssh://gw68.quarry.iu.teragrid.org"
HOST     = "ssh://repex1.tacc.utexas.edu"
HOST     = "ssh://boskop"

N_JS     =    1   # we iterate one more than that, to separate startup timing
N_J      = 2000
TIME     =    1


try:

    sumisc.benchmark_start (['job.Job run', HOST])

    for i in range (0, N_JS):
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
      # jd.number_of_processes = 1
        jd.arguments           = [TIME]
        jd.output              = "/tmp/saga_job.%s.stdout" % USER_ID
        jd.error               = "/tmp/saga_job.%s.stderr" % USER_ID

        for j in range (-1, N_J) :
            j = js.create_job(jd)
            j.run()
            sumisc.benchmark_tic ()

    sumisc.benchmark_eval ()
    sys.exit (0)

except saga.SagaException, ex:
    print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
    print " \n*** Backtrace:\n %s" % ex.traceback
    sys.exit (-1)

