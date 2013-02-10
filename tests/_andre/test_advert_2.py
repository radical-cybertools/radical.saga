
import sys
import time
import saga

d_1 = saga.advert.Directory ('redis://localhost/tmp/test1/test1/')
d_1.set_attribute ('foo', 'oops3')

sys.exit (0)

arg=""
if len (sys.argv) > 1 :
  arg=sys.argv[1]

print "start"
t1=time.time()

if arg == 'start' :
  d_1.set_attribute ('foo', 'start')

for i in xrange (10) :
  print i
  d_1.set_attribute ('foo', str(i))

t2=time.time()
print "done"

if arg == 'stop' :
  d_1.set_attribute ('foo', 'stop')

print (t2-t1)

