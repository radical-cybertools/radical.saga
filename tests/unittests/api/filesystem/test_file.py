
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import saga
import unittest

from copy import deepcopy

import radical.utils.testing as testing


class TestFile(unittest.TestCase):

    # -------------------------------------------------------------------------
    #
    def setUp(self):
        """Setup called once per class instance"""
        self.uniquefilename1 = "saga-unittests-1-"+str(os.getpid())
        self.uniquefilename2 = "saga-unittests-2-"+str(os.getpid())

    # -------------------------------------------------------------------------
    #
    def tearDown(self):
        """Teardown called once per class instance"""
        try:
            # do the cleanup
            tc = testing.get_test_config ()
            d = saga.filesystem.Directory(tc.filesystem_url)
            d.remove(self.uniquefilename1)
            d.remove(self.uniquefilename2)
        except saga.SagaException as ex:
            pass

    # -------------------------------------------------------------------------
    #
    def test_nonexisting_host_file_open(self):
        """ Testing if opening a file on a non-existing host causes an exception.
        """
        try:
            tc = testing.get_test_config ()
            invalid_url = deepcopy(saga.Url(tc.filesystem_url))
            invalid_url.host = "does.not.exist"
            f = saga.filesystem.File(invalid_url)
            assert False, "Expected BadParameter exception but got none."
        except saga.BadParameter:
            assert True
        except saga.SagaException as ex:
            assert False, "Expected BadParameter exception, but got %s" % ex


    # -------------------------------------------------------------------------
    #
    def test_nonexisting_file_open(self):
        """ Testing if opening a non-existing file causes an exception.
        """
        try:
            pass
            tc = testing.get_test_config ()
            nonex_file = deepcopy(saga.Url(tc.filesystem_url))
            nonex_file.path += "/file.does.not.exist"
            f = saga.filesystem.File(nonex_file)
            assert False, "Expected DoesNotExist exception but got none."
        except saga.DoesNotExist:
            assert True
        except saga.SagaException as ex:
            assert False, "Expected DoesNotExist exception, but got %s" % ex


    # -------------------------------------------------------------------------
    #
    def test_nonexisting_file_create_open(self):
        """ Testing if opening a non-existing file with the 'create' flag set works.
        """
        try:
            pass
            tc = testing.get_test_config ()
            nonex_file = deepcopy(saga.Url(tc.filesystem_url))
            nonex_file.path += "/%s" % self.uniquefilename1
            f = saga.filesystem.File(nonex_file, saga.filesystem.CREATE)
            assert f.size == 0  # this should fail if the file doesn't exist!
        except saga.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex


    # -------------------------------------------------------------------------
    #
    def test_existing_file_open(self):
        """ Testing if we can open an existing file.
        """
        try:
            tc = testing.get_test_config ()
            filename = deepcopy(saga.Url(tc.filesystem_url))
            filename.path += "/%s" % self.uniquefilename1
            f = saga.filesystem.File(filename, saga.filesystem.CREATE)

            f2 = saga.filesystem.File(f.url)
            assert f2.size == 0  # this should fail if the file doesn't exist!

        except saga.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_file_copy_invalid_tgt_scheme(self):
        """ Testing if get an exception if we try to copy an unsupported target scheme.
        """
        try:
            tc = testing.get_test_config ()

            # Create the source file
            source_file = deepcopy(saga.Url(tc.filesystem_url))
            source_file.path += "/%s" % self.uniquefilename1
            f1 = saga.filesystem.File(source_file, saga.filesystem.CREATE)

            target_url = deepcopy(saga.Url(tc.filesystem_url))
            target_url.scheme = "crapscheme"

            f1.copy(target_url)

            assert False, "Expected BadParameter exception but got none."
        except saga.BadParameter:
            assert True
        except saga.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_file_copy_1(self):
        """ Testing if we can copy an existing file.
        """
        try:
            pass
            tc = testing.get_test_config ()
            filename1 = deepcopy(saga.Url(tc.filesystem_url))
            filename1.path += "/%s" % self.uniquefilename1
            f1 = saga.filesystem.File(filename1, saga.filesystem.CREATE)

            filename2 = deepcopy(saga.Url(tc.filesystem_url))
            filename2.path += "/%s" % self.uniquefilename2

            f1.copy(filename2)
            f2 = saga.filesystem.File(filename2)
            assert f2.size == 0  # this should fail if the file doesn't exist!

        except saga.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex




