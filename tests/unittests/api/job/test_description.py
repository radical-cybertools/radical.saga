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
        jd1.executable = '/bin/true'
        jd2 = jd1.clone ()
        jd2.executable = '/bin/false'
        assert jd1.executable != jd2.executable 

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
