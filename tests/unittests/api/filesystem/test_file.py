__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import os
import sys
import saga
import saga.utils.test_config as sutc

from copy import deepcopy


# ------------------------------------------------------------------------------
#
def test_nonexisting_host_file_open():
    """ Testing if opening a file on a non-existing host causes an exception.
    """
    try:
        pass
        tc = sutc.TestConfig()
        invalid_url = deepcopy(saga.Url(tc.filesystem_url))
        invalid_url.host += ".does.not.exist"
        f = saga.filesystem.File(invalid_url)
        assert False, "Expected BadParameter exception but got none."
    except saga.BadParameter:
        assert True
    except saga.SagaException as ex:
        assert False, "Expected BadParameter exception, but got %s" % ex


# ------------------------------------------------------------------------------
#
def test_nonexisting_file_open():
    """ Testing if opening a non-existing file causes an exception.
    """
    try:
        pass
        tc = sutc.TestConfig()
        nonex_file = deepcopy(saga.Url(tc.filesystem_url))
        nonex_file.path += "/file.does.not.exist"
        f = saga.filesystem.File(nonex_file)
        assert False, "Expected DoesNotExist exception but got none."
    except saga.DoesNotExist:
        assert True
    except saga.SagaException as ex:
        assert False, "Expected DoesNotExist exception, but got %s" % ex


# ------------------------------------------------------------------------------
#
def test_nonexisting_file_create_open():
    """ Testing if opening a non-existing file with the 'create' flag set works.
    """
    try:
        pass
        tc = sutc.TestConfig()
        nonex_file = deepcopy(saga.Url(tc.filesystem_url))
        nonex_file.path += "/file.does.not.exist"
        f = saga.filesystem.File(nonex_file, saga.filesystem.CREATE)
        assert f.size == 0
    except saga.SagaException as ex:
        assert False, "Unexpected exception: %s" % ex


# ------------------------------------------------------------------------------
#
def test_existing_file_open():
    """ Testing if we can open an existing file.
    """
    try:
        pass
        #tc = sutc.TestConfig()
        #invalid_url       = deepcopy(saga.Url(tc.js_url))
        #invalid_url.host += ".does.not.exist"
        #js = saga.job.Service(invalid_url, tc.session)
        #assert False, "Expected XYZ exception but got none."

    except saga.SagaException as ex:
        assert False, "Unexpected exception: %s" % ex
