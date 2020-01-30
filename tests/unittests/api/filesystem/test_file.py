
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import unittest

from copy import deepcopy

import radical.saga  as rs
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def config():

    ru.set_test_config(ns='radical.saga')
    ru.add_test_config(ns='radical.saga', cfg_name='file_localhost')

    return ru.get_test_config()


# ------------------------------------------------------------------------------
#
class TestFile(unittest.TestCase):

    # -------------------------------------------------------------------------
    #
    def setUp(self):
        """Setup called once per class instance"""
        self.uniquefilename1 = "saga-unittests-1-" + str(os.getpid())
        self.uniquefilename2 = "saga-unittests-2-" + str(os.getpid())

    # -------------------------------------------------------------------------
    #
    def tearDown(self):
        """Teardown called once per class instance"""
        try:
            # do the cleanup
            tc = config()
            d = rs.filesystem.Directory(tc.filesystem_url)
            d.remove(self.uniquefilename1)
            d.remove(self.uniquefilename2)
        except rs.SagaException:
            pass

    # -------------------------------------------------------------------------
    #
    def test_nonexisting_host_file_open(self):
        """ Testing if opening a file on a non-existing host causes an exception.
        """
        try:
            tc = config()
            invalid_url = deepcopy(rs.Url(tc.filesystem_url))
            invalid_url.host = "does/not/exist"
            _ = rs.filesystem.File(invalid_url)
            assert False, "Expected BadParameter exception but got none."
        except rs.DoesNotExist:
            assert True
        except rs.SagaException as ex:
            assert False, "Expected BadParameter exception, but got %s" % ex


    # -------------------------------------------------------------------------
    #
    def test_nonexisting_file_open(self):
        """ Testing if opening a non-existing file causes an exception.
        """
        try:
            pass
            tc = config()
            nonex_file = deepcopy(rs.Url(tc.filesystem_url))
            nonex_file.path += "/file.does.not.exist"
            _ = rs.filesystem.File(nonex_file)
            assert False, "Expected DoesNotExist exception but got none."
        except rs.DoesNotExist:
            assert True
        except rs.SagaException as ex:
            assert False, "Expected DoesNotExist exception, but got %s" % ex


    # -------------------------------------------------------------------------
    #
    def test_nonexisting_file_create_open(self):
        """ Testing if opening a non-existing file with the 'create' flag set works.
        """
        try:
            pass
            tc = config()
            nonex_file = deepcopy(rs.Url(tc.filesystem_url))
            nonex_file.path += "/%s" % self.uniquefilename1
            f = rs.filesystem.File(nonex_file, rs.filesystem.CREATE)
            assert f.size == 0  # this should fail if the file doesn't exist!
        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex


    # -------------------------------------------------------------------------
    #
    def test_existing_file_open(self):
        """ Testing if we can open an existing file.
        """
        try:
            tc = config()
            filename = deepcopy(rs.Url(tc.filesystem_url))
            filename.path += "/%s" % self.uniquefilename1
            f = rs.filesystem.File(filename, rs.filesystem.CREATE)

            f2 = rs.filesystem.File(f.url)
            assert f2.size == 0  # this should fail if the file doesn't exist!

        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_file_copy_invalid_tgt_scheme(self):
        """ Testing if get an exception if we try to copy an unsupported target scheme.
        """
        try:
            tc = config()

            # Create the source file
            source_file = deepcopy(rs.Url(tc.filesystem_url))
            source_file.path += "/%s" % self.uniquefilename1
            f1 = rs.filesystem.File(source_file, rs.filesystem.CREATE)

            target_url = deepcopy(rs.Url(tc.filesystem_url))
            target_url.scheme = "crapscheme"

            f1.copy(target_url)

            assert False, "Expected BadParameter exception but got none."
        except rs.BadParameter:
            assert True
        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_file_copy_1(self):
        """ Testing if we can copy an existing file.
        """
        try:
            pass
            tc = config()
            filename1 = deepcopy(rs.Url(tc.filesystem_url))
            filename1.path += "/%s" % self.uniquefilename1
            f1 = rs.filesystem.File(filename1, rs.filesystem.CREATE)

            filename2 = deepcopy(rs.Url(tc.filesystem_url))
            filename2.path += "/%s" % self.uniquefilename2

            f1.copy(filename2)
            f2 = rs.filesystem.File(filename2)
            assert f2.size == 0  # this should fail if the file doesn't exist!

        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_file_copy_remote_local(self):
        """ Testing if we can copy a file from remote -> local
        """
        try:
            pass
            tc = config()
            filename1 = deepcopy(rs.Url(tc.filesystem_url))
            filename1.path = "/etc/passwd"
            f1 = rs.filesystem.File(filename1)

            filename2 = "file://localhost/tmp/%s" % self.uniquefilename2

            f1.copy(filename2)
            f2 = rs.filesystem.File(filename2)
            assert f2.size != 0  # this should fail if the file doesn't exist!

        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_file_get_session(self):
        """ Testing if the correct session is being used
        """
        try:
            tc = config()
            session = tc.session or rs.Session()
            filename = deepcopy(rs.Url(tc.filesystem_url))
            filename.path += "/%s" % self.uniquefilename1

            f = rs.filesystem.File(filename, rs.filesystem.CREATE,
                                     session=session)
            assert f.get_session() == session, 'Failed setting the session'

            f2 = rs.filesystem.File(f.url, session=session)
            assert f2.get_session() == session, 'Failed setting the session'

        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s" % ex

    # -------------------------------------------------------------------------
    #
    def test_directory_get_session(self):
        """ Testing if the correct session is being used
        """
        try:
            tc = config()
            session = tc.session or rs.Session()

            d = rs.filesystem.Directory(tc.filesystem_url, session=session)

            assert d.get_session() == session, 'Failed setting the session'

        except rs.SagaException as ex:
            assert False, "Unexpected exception: %s [%s]" % (ex,
                    tc.filesystem_url)

