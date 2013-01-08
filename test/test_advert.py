
import sys
import random
import time
import saga

from   saga.engine.engine import Engine

class my_cb (saga.Callback) :

  def __init__ (self) :
    self.t1  = 0
    self.t2  = 0
    self.cnt = 0


  def cb (self, obj, key, val) :
    # print " ----------------- callback triggered for %s - %s - %s" % (obj, key, val)
    self.cnt += 1
    if val == 'start' :
      print 'start'
      self.t1 = time.time()
    if val == 'stop' :
      print 'stop'
      self.t2 = time.time()
      print self.cnt
      print (self.t2-self.t1)



try :

# e = Engine ()
# e._dump()
  
  d_1 = saga.advert.Directory ('redis://:securedis@localhost/tmp/test1/test1/',
                                   saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS)
  print d_1
  print d_1.get_url ()
  d_1.set_attribute ('foo', 'bar')
  print d_1.get_attribute ('foo')
  d_1.foo = 'baz'
  print d_1.foo

  d_1.add_callback ('foo', my_cb ())



  e_1 = saga.advert.Entry ('redis://:securedis@localhost/tmp/test1/test1/passwd', 
                           saga.filesystem.CREATE)
  print e_1
  print e_1.get_url ()
  e_1.set_attribute ('foo', 'bar')
  print e_1.get_attribute ('foo')
  e_1.foo = 'baz'
  print e_1.foo

  e_2 = saga.advert.Entry ('redis://:securedis@localhost/tmp/test1/test1/groups', 
                           saga.filesystem.CREATE)
  print e_2
  print e_2.get_url ()
  e_2.set_attribute ('foo', 'bar')
  print e_2.get_attribute ('foo')
  e_2.foo = 'buz'
  print e_2.foo

  time.sleep (100)

  sys.exit (0)

  
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
  
  
  print f_2.get_size_self ()
  t_2 = f_2.get_size_self (saga.task.ASYNC)
  print t_2.state
  print t_2.result
  
  # f_2.copy_self ('passwd.bak') 
  f_2.copy_self ('dummy://boskop/tmp/') 
  
  
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
  
