__author__    = ["Ole Weidner", "Andre Merzky"]
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import os
import sys
import saga
import saga.utils.test_config as sutc


# ------------------------------------------------------------------------------
#
def test_deepcopy():

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
