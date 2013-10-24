
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import gc
import sys
import saga
import time
import objgraph as checks
from   pprint import pprint as pp

# gc.set_debug (gc.DEBUG_LEAK | gc.DEBUG_STATS)

def func () :
    js1 = saga.job.Service ('ssh://localhost/bin/sh')


##  js0 = saga.job.Service ('ssh://localhost/bin/sh')
##  t0  = saga.job.Service.create ('ssh://localhost/bin/sh')
##  t0.wait ()
##  js0 = t0.get_result ()
##  print js0.url 

print "--------------------------"
func ()
print "--------------------------"
gc.collect ()

# print "=============================="
# for g in gc.garbage :
#     print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
#     pp (g)
# print "=============================="
# pp (gc.garbage)
# print "=============================="

# checks.show_backrefs (js1._adaptor)
print checks.count('ShellJobService')
checks.show_backrefs(checks.by_type('ShellJobService')[-1])




