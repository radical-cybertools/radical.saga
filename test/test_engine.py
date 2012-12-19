
import saga

try :

  d = saga.filesystem.Directory ('file://localhost/etc/passwd')

  jd     = saga.job.Description ()
  jd.executable = '/bin/date'

  # t_1    = saga.job.Service.create ("local://localhost", ttype=saga.task.TASK)
  # print str(t_1)

  js_1   = saga.job.Service ("fork://localhost")
  print js_1.get_url ()

  j_1    = js_1.create_job (jd) 
  print str(j_1)
  print j_1.get_id ()

  # t_2    = j_1.get_id (ttype=saga.task.TASK)
  # print str(t_2)
  # print t_2.get_state ()

  s = saga.Session ()

  c = saga.Context ('MyProxy')
  c.user_id   = 'merzky'
  c.user_pass = 'secret'
  c.life_time = 1000
  c.server    = 'myproxy.teragrid.org:7514'

  s.add_context (c)

  for ctx in s.contexts :
    print ctx._attributes_dump()




except saga.exceptions.SagaException as e :
  print "Exception: ==========\n%s"  %  e.get_message ()
  print "%s====================="    %  e.get_traceback ()

