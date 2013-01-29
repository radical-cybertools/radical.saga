
import gc
import os
import sys
import time
import saga
import pprint
import cProfile

import subprocess

import threading

def get_mem () :

    pid     = os.getpid()
    process = subprocess.Popen ("ps h -p %s -o rss" % pid,
                                shell=True, stdout=subprocess.PIPE)
    stdout_list = process.communicate ()[0].split('\n')
    return stdout_list[0]
    return 0

class workload :

    cnt = 0

    def __init__ (self, service_url, s, n_jobs) :
    
        jd   = saga.job.Description ()
        jd.executable = '/bin/date'
        
        service = saga.job.Service (service_url, session=s)

        print "~~~~~~~~~~~~~~~~~"
        pprint.pprint (gc.get_referrers(service))
        print "~~~~~~~~~~~~~~~~~"
        pprint.pprint (gc.get_referents(service))
        print "~~~~~~~~~~~~~~~~~"
    
        for i in range (0,n_jobs) :
            tmp_j  = service.create_job (jd)
            tmp_j.run ()
            workload.cnt += 1
            print "%5d : %s" % (workload.cnt, tmp_j.id)

        del (service)


def main () :
    try :

        gc.enable ()
      # gc.set_debug (gc.DEBUG_STATS         | gc.DEBUG_COLLECTABLE \
      #             | gc.DEBUG_UNCOLLECTABLE | gc.DEBUG_INSTANCES | gc.DEBUG_OBJECTS)
        print gc.get_threshold()
        

        if len (sys.argv) < 4:
            sys.exit ('\n\tusage: %s n_jobs n_services url\n' % sys.argv[0])
    
        start = time.time ()

        s = saga.Session ()
        c = saga.Context ('ssh')
        c.user_id = 'merzky'
    
        s.add_context (c)

        n_jobs      = int (sys.argv[1])
        n_services  = int (sys.argv[2])
        service_url = str (sys.argv[3])
        threads     = []

        for _i in range (0, n_services) :
            thread = threading.Thread (target=workload, args=[service_url, s, n_jobs//n_services])
            thread.start ()
            threads.append (thread)

        for thread in threads :
            thread.join ()

        stop = time.time ()
        print "-----------------------------------------------"
        print "services: %s" % (n_services)
        print "jobs    : %s" % (n_jobs)
        print "time    : %s" % (stop - start)
        print "memory  : %s" % (get_mem ())
        print "jobs/sec: %s" % (n_jobs / (stop - start))
            
    except saga.exceptions.SagaException as e :
        print "Exception: ==========\n%s"  %  e.get_message ()
        print "%s====================="    %  e.get_traceback ()


# cProfile.run('main()', 'test_perf.prof')
main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

