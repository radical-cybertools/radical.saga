
import saga

e = saga.engine.Engine ()

e.list_loaded_adaptors ()

# a1 = e.get_adaptor ('saga.job.Job',     'fork', saga.task.SYNC)
# a2 = e.get_adaptor ('saga.job.Service', 'fork', saga.task.SYNC, 'fork://localhost/')
# 
# a3 = e.get_adaptor ('saga.job.Job',     'oops', saga.task.SYNC)
# a4 = e.get_adaptor ('saga.job.Service', 'oops', saga.task.SYNC, 'oops://localhost/')

js = saga.job.Service ("fork://localhost")
print js.get_url ()

