
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import gc
import sys
import saga
from   pprint import pprint as pp

try :

    if True :
        js1 = saga.job.Service ('ssh://localhost/bin/sh')
        print sys.getrefcount (js1)
        pp (gc.get_referrers (js1))

except saga.SagaException as e :
    print str(e)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

