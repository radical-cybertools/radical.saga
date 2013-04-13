
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import gc
import sys
import saga
import time
import objgraph as og
from   pprint import pprint as pp

# gc.set_debug (gc.DEBUG_LEAK | gc.DEBUG_STATS)


## if True :
##     js0 = saga.job.Service ('ssh://localhost/bin/sh')
##     t0  = saga.job.Service.create ('ssh://localhost/bin/sh')
##     t0.wait ()
##     js0 = t0.get_result ()
##     print js0.url 
## 
## gc.collect ()
## pp (js0.__dict__)
## print "---------------"
## # pp (js0.__dict__['_init_task'].__dict__)
## 
## sys.exit (0)


if True :
    js1 = saga.job.Service ('ssh://localhost/bin/sh')
    js2 = js1
    # refs = gc.get_referrers (js1)

    # for ref in refs :
    #     print "------------------------------"
    #     print type (ref)
    #     pp (ref)

 #  js1._adaptor.__del__()
    
time.sleep (5)
gc.collect ()
# print "=============================="
# for g in gc.garbage :
#     print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
#     pp (g)
# print "=============================="
# pp (gc.garbage)
# print "=============================="

og.show_most_common_types ()
print type (js1._adaptor)
print type (js2._adaptor)
# og.show_backrefs (js1._adaptor)
# og.show_backrefs (js2._adaptor)
print og.count('saga.job.Service')

## pp (js1.__dict__)
## # print "--------------------------------------------------------------------------------"
## # pp (type(js1.__dict__['_adaptor']))
## # pp (js1.__dict__['_adaptor'].__dict__)
## print "--------------------------------------------------------------------------------"
## # pp (type(js1.__dict__['_adaptor'].__dict__['_adaptor']))
## # pp (js1.__dict__['_adaptor'].__dict__['_adaptor'].__dict__)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

