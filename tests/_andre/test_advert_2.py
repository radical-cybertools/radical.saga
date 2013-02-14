
import sys
import saga

start = False
stop  = False

for arg in sys.argv :
    if arg == 'start' : start = True
    if arg == 'stop'  : stop  = True

# open an advert dir, and update an atrtribute as quickly as possible
# d_1 = saga.advert.Directory ('redis://localhost/tmp/test1/test1/')
d_1 = saga.advert.Directory ('redis://repex1.tacc.utexas.edu:10001/tmp/test1/test1/')

print "1"
# should we notify the master that we are about to start?
if start : d_1.set_attribute ('foo', 'start')
print "2"

# send 1000 attribute updates
for i in xrange (10) :
    d_1.set_attribute ('foo', str(i))
    print "."

# should we notify the master that we are done?
if stop : d_1.set_attribute ('foo', 'stop')


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

