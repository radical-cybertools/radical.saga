
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import sys
import saga
import saga.utils.test_config as sutc


# ------------------------------------------------------------------------------
#
def test_deepcopy():
    """ Test deep copy """

    try:
        jd1 = saga.job.Description ()
        jd1.executable = '/bin/sleep'
        jd1.arguments  = ['1.3']
        jd2 = jd1.clone ()
        jd2.executable = '/bin/nanosleep'
        assert jd1.executable != jd2.executable, "%s != %s" % (jd1.executable, jd2.executable)
        assert jd1.arguments  == jd2.arguments 

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se

def test_environment ():
    """ Test environment type conversion """

    target = {'A': 'a', 'B': 'b'}

    jd = saga.job.Description ()
    jd.environment = ['A=a', 'B=b']
    assert (jd.environment == target), "'%s' == '%s'" % (jd.environment, target)
    
    jd = saga.job.Description ()
    jd.environment = {'A':'a', 'B':'b'}
    assert (jd.environment == target), "'%s' == '%s'" % (jd.environment, target)
    
    jd = saga.job.Description ()
    jd.environment = 'A=a, B=b'
    assert (jd.environment == target), "'%s' == '%s'" % (jd.environment, target)
    
    jd = saga.job.Description ()
    jd.environment = 'A=a:B=b'
    assert (jd.environment == target), "'%s' == '%s'" % (jd.environment, target)
    
    try :
        jd = saga.job.Description ()
        jd.environment = 1
        assert (False), "expected BadParameter exception, got none"
    except saga.BadParameter :
        assert (True)
    except saga.SagaException as se:
        assert (False), "expected BadParameter exception, got %s" % se
    
