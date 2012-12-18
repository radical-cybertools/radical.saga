
import saga

try :
  e = saga.engine.Engine ()
  
  # a_1 = e.get_adaptor ('saga.job.Job',     'fork', saga.task.SYNC)
  # a_2 = e.get_adaptor ('saga.job.Service', 'fork', saga.task.SYNC, 'fork://localhost/')
  # 
  # a_3 = e.get_adaptor ('saga.job.Job',     'oops', saga.task.SYNC)
  # a_4 = e.get_adaptor ('saga.job.Service', 'oops', saga.task.SYNC, 'oops://localhost/')
  
  jd     = saga.job.Description ()
  jd.executable = '/bin/date'


  task_1 = saga.job.create_service ("local://localhost", ttype=saga.task.TASK)
  print str(task_1)

  js_1   = saga.job.Service ("fork://localhost")
  print js_1.get_url ()

  j_1    = js_1.create_job (jd) 
  print str(j_1)
  print j_1.get_id ()


except saga.exceptions.SagaException as e :
  print "Exception: ==========\n%s"  %  e.get_message ()
  print "%s====================="    %  e.get_traceback ()

