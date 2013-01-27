
import os
import sys
import time
import saga
import cProfile

import subprocess

def get_mem () :

    # pid     = os.getpid()
    # process = subprocess.Popen ("ps h -p %s -o rss" % pid,
    #                             shell=True, stdout=subprocess.PIPE)
    # stdout_list = process.communicate ()[0].split('\n')
    # return stdout_list[0]
    return 0


def main () :
    try :
    
      # e = saga.engine.engine.Engine ()
      # e._dump ()
    
      print "mem  init : %s" % (get_mem())
      print "time init : %s" % ('0.0')
      start = time.time ()
    
      s = saga.Session ()
      c = saga.Context ('ssh')
      c.user_id = 'amerzky'
    
      s.add_context (c)
    
      jd   = saga.job.Description ()
      jd.executable = '/bin/date'
    
    # for i in range (1, 3) :
      tmp_js = saga.job.Service ("ssh://amerzky@cyder.cct.lsu.edu", session=s)
      for i in range (1, 1000) :
          tmp_j  = tmp_js.create_job (jd)
          tmp_j.run ()
    
      sys.exit (0)

      print "mem  check: %s" % (get_mem())
      print "time check: %s" % (time.time () - start)

    
      jobs = []
      js   = saga.job.Service ("ssh://localhost")
      for i in range (1, 10000) :
          j = js.create_job (jd) 
          j.run ()
          jobs.append (j)
          if not i % 1000 :
              print "%5d: %s" % (i, get_mem())
              
      for job in jobs :
          jid = job.get_id ()
      
      print "mem  final: %s" % (get_mem())
      print "time final: %s" % (time.time () - start)
          
    except saga.exceptions.SagaException as e :
      print "Exception: ==========\n%s"  %  e.get_message ()
      print "%s====================="    %  e.get_traceback ()

# cProfile.run('main()', 'test_perf.prof')
main()


