
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for saga.utils.signatures
"""

import saga

def test_signatures () :
    """ Test if signature violations are flagged """ 

    try :
        s = saga.Session ('should not accept a string')
        assert False, "should have seen a BadParameter exception"
    except saga.BadParameter as e :
        assert True
      # print str(e)
    except Exception as e : 
        assert False, "should have seen a BadParameter exception, not %s" % e

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

