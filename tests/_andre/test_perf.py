
import os
import sys
import time
import saga
import cProfile

import subprocess

import threading

def get_mem () :

    # pid     = os.getpid()
    # process = subprocess.Popen ("ps h -p %s -o rss" % pid,
    #                             shell=True, stdout=subprocess.PIPE)
    # stdout_list = process.communicate ()[0].split('\n')
    # return stdout_list[0]
    return 0

def workload (service_url, s, n_jobs) :

    jd   = saga.job.Description ()
    jd.executable = '/bin/date'
    
    print 4
    service = saga.job.Service (service_url, session=s)
    print 5

    for i in range (0,n_jobs) :
        tmp_j  = service.create_job (jd)
        tmp_j.run ()
    

def main () :
    try :

        if len (sys.argv) < 4:
            sys.exit ('\n\tusage: %s n_jobs n_services url\n' % sys.argv[0])
    
        start = time.time ()

        print 1
    
        s = saga.Session ()
        c = saga.Context ('ssh')
        c.user_id = 'amerzky'
    
        print 2
        s.add_context (c)

        n_jobs      = int (sys.argv[1])
        n_services  = int (sys.argv[2])
        service_url = str (sys.argv[3])
        threads     = []

        print 3
        for _i in range (0, n_services) :
            thread = threading.Thread (target=workload, args=[service_url, s, n_jobs//n_services])
            thread.start ()
            threads.append (thread)

        for thread in threads :
            thread.join ()

        print "services: %s" % (n_services)
        print "jobs    : %s" % (n_jobs)
        print "time    : %s" % (time.time () - start)
        print "memory  : %s" % (get_mem ())
            
    except saga.exceptions.SagaException as e :
        print "Exception: ==========\n%s"  %  e.get_message ()
        print "%s====================="    %  e.get_traceback ()

# cProfile.run('main()', 'test_perf.prof')
main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

