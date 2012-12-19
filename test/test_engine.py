
import saga

try :

  c = saga.Context ('UserPass')


  jd     = saga.job.Description ()
  jd.executable = '/bin/date'

  t_1    = saga.job.Service.create ("local://localhost", ttype=saga.task.TASK)
  print str(t_1)

  js_1   = saga.job.Service ("fork://localhost")
  print js_1.get_url ()

  j_1    = js_1.create_job (jd) 
  print str(j_1)
  print j_1.get_id ()

  t_2    = j_1.get_id (ttype=saga.task.TASK)
  print str(t_2)
  print t_2.get_state ()




except saga.exceptions.SagaException as e :
  print "Exception: ==========\n%s"  %  e.get_message ()
  print "%s====================="    %  e.get_traceback ()

