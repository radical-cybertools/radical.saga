
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import time
import saga
import saga.utils.test_config as sutc

from copy import deepcopy


# ------------------------------------------------------------------------------
#
def test_replica_entry():
    """ Test logical file entry 
    """
    try:
        tc = sutc.TestConfig()
        the_url = tc.js_url # from test config file
        the_session = tc.session # from test config file

        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex

# ------------------------------------------------------------------------------
#
def test_replica_directory():
    """ Test logical file directory 
    """
    try:
        tc = sutc.TestConfig()
        the_url = tc.js_url # from test config file
        the_session = tc.session # from test config file

        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex


