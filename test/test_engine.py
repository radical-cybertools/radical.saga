
import saga.engine

e = saga.engine.Engine ()

e.list_loaded_adaptors ()

a1 = e.get_adaptor ('saga.job.Job',     'fork')
a2 = e.get_adaptor ('saga.job.Service', 'fork', 'fork://localhost/')

a3 = e.get_adaptor ('saga.job.Job',     'oops')
a4 = e.get_adaptor ('saga.job.Service', 'oops', 'oops://localhost/')

