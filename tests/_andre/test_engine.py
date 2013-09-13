
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import random
import saga
import time

from   saga.engine.engine import Engine


try :

  print " ----------------------------------------------------------- "
  t1 = time.time ()
  js = saga.job.Service ("fork://localhost")
  # tc = saga.task.Container ()
  jd = saga.job.Description()
  jd.executable  = '/bin/sleep'
  jd.arguments   = ["1"]
  for i in range (1, 10) :
      j = js.create_job (jd)
      j.run ()

  
  # tc.run  ()
  # tc.wait (saga.task.ALL)
  
  # for t in tc.tasks :
  #   print "%s : %-6s [%s]"  %  (t, t.state, t.exception)
  t2 = time.time ()

  print " ----------------------------------------------------------- "
  print (t2-t1)
  print " =========================================================== "

  sys.exit (0)




  e = Engine ()
  e._dump()
  
  d_1 = saga.filesystem.Directory ('file://localhost/tmp/test1/test1/',
                                   saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS)
  print d_1
  print d_1.get_url ()
  
  
  t_0 = saga.filesystem.Directory.create ('file://localhost/tmp/test1/test1/',
                                          saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS, 
                                          ttype=saga.task.TASK)
  print t_0
  print t_0.state
  t_0.run ()
  print t_0.state
  t_0.wait ()
  print t_0.state
  d_2 = t_0.result
  print d_2
  print d_2.get_url()
  
  
  
  t_1 = d_1.open ('group', ttype=saga.task.TASK)
  print "New     : %s" % t_1.state
  t_1.run ()
  print "Running : %s" % t_1.state
  t_1.wait ()
  print "Done    : %s" % t_1.state
  f_1 = t_1.result
  print f_1.url
  print f_1.size
  
  print d_1.get_url ()
  f_2 = d_1.open ('passwd')
  
  f_2._adaptor._dump()
  
  tc = saga.task.Container ()
  
  for i in range (1, 10) :
    t = d_1.copy ("/etc/passwd", "/tmp/test_a_%04d.bak"  %  i, ttype=saga.task.TASK)
    tc.add (t)
  
  
  js = saga.job.Service ("fork://localhost")
  for i in range (1, 10) :
      jd = saga.job.Description()
      jd.executable  = '/usr/bin/touch'
      jd.arguments   = ["/tmp/test_b_%04d.bak"  %  i]
      tc.add (js.create_job (jd))
  
  tc.run  ()
  tc.wait (saga.task.ALL)
  
  for t in tc.tasks :
    print "%s : %-6s [%s]"  %  (t, t.state, t.exception)
  
  
  print f_2.get_size ()
  t_2 = f_2.get_size (saga.task.ASYNC)
  print t_2.state
  print t_2.result
  
  # f_2.copy ('passwd.bak') 
  f_2.copy ('dummy://boskop/tmp/') 
  
  
  t_3 = saga.filesystem.Directory.create ('file://localhost/tmp/test1/test1/',
                                 saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS, saga.task.ASYNC)
  print t_3
  print t_3.state
  d2 = t_3.get_result ()
  print d2
  
  
  jd     = saga.job.Description ()
  jd.executable = '/bin/date'
  
  # t_4    = saga.job.Service.create ("local://localhost")
  # print str(t_4)
  
  js_1   = saga.job.Service ("fork://localhost")
  print js_1.get_url ()
  
  j_1    = js_1.create_job (jd) 
  print str(j_1)
  print j_1.get_id ()
  
  # t_5    = j_1.get_id (ttype=saga.task.TASK)
  # print str(t_5)
  # print t_5.get_state ()
  
  s = saga.Session (default=True)
  
  # c = saga.Context ('MyProxy')
  # c.user_id   = 'merzky'
  # c.user_pass = 'I80PudW.'
  # c.life_time = 1000
  # c.server    = 'myproxy.teragrid.org:7514'
  # 
  # s.add_context (c)
  
  for ctx in s.contexts :
    ctx._attributes_dump()
  
  
  
  
except saga.exceptions.SagaException as e :
  print "Exception: ==========\n%s"  %  e.get_message ()
  print "%s====================="    %  e.get_traceback ()
  
