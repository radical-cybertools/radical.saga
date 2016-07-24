
from __future__ import absolute_import
from __future__ import print_function
import saga.utils.signatures          as sus
import saga.adaptors.base             as sab
import saga.adaptors.shell.shell_file as sasf

import saga
js = saga.job.Service ('ssh://localhost/')
j  = js.run_job ("/bin/date")

@sus.takes (sab.Base)
def method (adaptor) :
  print()
  print("-----------------------------------------------")
  print(type (adaptor))
  print(adaptor.__class__.__mro__)
  print("-----------------------------------------------")
  print()

a = sasf.Adaptor ()
method (a)

