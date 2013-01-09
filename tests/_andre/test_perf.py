
import os
import saga

import subprocess

def get_mem () :

    pid     = os.getpid()
    process = subprocess.Popen ("ps h -p %s -o rss" % pid,
                                shell=True, stdout=subprocess.PIPE)
    stdout_list = process.communicate ()[0].split('\n')
    return stdout_list[0]


try :

  print ""
  print "init : %s" % (get_mem())

  jd   = saga.job.Description ()
  jd.executable = '/bin/date'

  for i in range (0, 10001) :
      tmp = saga.job.Service ("fork://localhost")

  print "check: %s" % (get_mem())

  jobs = []
  js   = saga.job.Service ("fork://localhost")
  for i in range (1, 10001) :
      j = js.create_job (jd) 
      jobs.append (j)
      if not i % 1000 :
          print "%5d: %s" % (i, get_mem())
          
  for job in jobs :
      jid = job.get_id ()
  
  print "final: %s" % (get_mem())
      
except saga.exceptions.SagaException as e :
  print "Exception: ==========\n%s"  %  e.get_message ()
  print "%s====================="    %  e.get_traceback ()

