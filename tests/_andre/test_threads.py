
from __future__ import absolute_import
from __future__ import print_function
import time
import saga.utils.threads as sut
from six.moves import range

def work () :

  for i in range (0, 10) :
    print(i)
    time.sleep (1)
  return "hello world"

t = sut.Thread (work)
time.sleep (1)
print('-- 0 ' + t.state)

t.start ()
time.sleep (1)
print('-- 1 ' + t.state)

t.wait ()
print('-- 3 ' + t.state)

print(t.result)

