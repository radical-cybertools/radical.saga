
from __future__ import absolute_import
from __future__ import print_function
import saga
from six.moves import range

files = []
for i in range (0, 100) :
  f = saga.filesystem.File ('ssh://boskop/etc/passwd')
  print("%3d: %d"  %  (i, f.get_size ()))
  files.append (f)
  f.close()

