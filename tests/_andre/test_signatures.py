
import radical.saga.utils.signatures          as sus
import radical.saga.adaptors.base             as sab
import radical.saga.adaptors.shell.shell_file as sasf

import radical.saga as saga
js = saga.job.Service ('ssh://localhost/')
j  = js.run_job ("/bin/date")

@sus.takes (sab.Base)
def method (adaptor) :
  print
  print "-----------------------------------------------"
  print type (adaptor)
  print adaptor.__class__.__mro__
  print "-----------------------------------------------"
  print

a = sasf.Adaptor ()
method (a)

