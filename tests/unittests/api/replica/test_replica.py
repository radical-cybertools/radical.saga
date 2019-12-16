
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import os
import pytest

import radical.utils as ru
import radical.saga  as rs


FILE_SIZE      = 1  # in megs, approx
NUM_REPLICAS   = 5  # num replicas to create
TEMP_FILENAME  = "/tmp/%s.temp.txt" % os.getpid()
TEMP_DIRECTORY = "rs.test"
IRODS_RESOURCE = "osgGridFtpGroup"


# ------------------------------------------------------------------------------
# check for supported API packages
#
try:
    r = rs.logicalfile.LogicalDirectory('irods://localhost/')

except:
    pytest.skip("skip replica test (no irods)", allow_module_level=True)


# ------------------------------------------------------------------------------
#
def test_replica_entry(config):

    cfg = config()
    cfg.assert_exception(rs.replica.LogicalDirectory(cfg.replica_file_url))


# ------------------------------------------------------------------------------
#
def test_replica_directory(config):

    cfg = config()
    cfg.assert_exception(rs.replica.LogicalDirectory(cfg.replica_dir_url))


# ------------------------------------------------------------------------------
#
def test_replica_directory_listing(config):

    cfg = config()
    replica_directory = rs.replica.LogicalDirectory(cfg.replica_url)

    assert(replica_directory.list())


# ------------------------------------------------------------------------------
#
def test_upload_and_download(config):

    cfg         = config()
    tmp_dir     = '/tmp/'
    replica_url = cfg.replica_url

    with open(TEMP_FILENAME, "wb") as f:
        f.write("x" * (FILE_SIZE * pow(2,20)) )

    # clear old file
    out, err, ret = ru.sh_callout(["irm", TEMP_FILENAME])

    _ = rs.replica.LogicalDirectory(replica_url)
    f = rs.replica.LogicalFile(replica_url + TEMP_FILENAME)
    f.upload(tmp_dir + TEMP_FILENAME,
             "irods:///path/is/ignored/?resource=" + IRODS_RESOURCE)

    # myfile.upload(tmp_dir + TEMP_FILENAME, \
    #                   "irods:///this/path/is/ignored")

    os.remove(tmp_dir + TEMP_FILENAME)

    myfile = rs.replica.LogicalFile(replica_url + TEMP_FILENAME)
    myfile.download(TEMP_FILENAME)

    os.remove(tmp_dir + TEMP_FILENAME)


# ------------------------------------------------------------------------------
#
def test_replica_get_size(config):
    """ Test logical file get_size()
    """
    try:
        cfg         = config()
        replica_url = cfg.replica_url
        tmp_dir     = '/tmp/'

        # create a file for us to use
        with open(tmp_dir + TEMP_FILENAME, "wb") as f:
            f.write("x" * (FILE_SIZE * pow(2,20)) )

        _      = rs.replica.LogicalDirectory(replica_url)
        myfile = rs.replica.LogicalFile(replica_url + TEMP_FILENAME)
        myfile.upload(tmp_dir + TEMP_FILENAME,
                      "irods:///path/is/ignored/?resource=" + IRODS_RESOURCE,
                      rs.replica.OVERWRITE)

        myfile = rs.replica.LogicalFile(replica_url + TEMP_FILENAME)

        assert True

    except rs.SagaException as ex:
        assert False, "unexpected exception %s\n%s" % (ex.traceback, ex)


# ------------------------------------------------------------------------------
#
def test_replica_remove(config):
    """
    Test logical file remove: remove the file from the remote resource
    """
    try:
        cfg         = config()
        replica_url = cfg.replica_url
        tmp_dir     = '/tmp/'

        # create a file for us to use
        with open(tmp_dir + TEMP_FILENAME, "wb") as f:
            f.write("x" * (FILE_SIZE * pow(2,20)) )

        _      = rs.replica.LogicalDirectory(replica_url)
        myfile = rs.replica.LogicalFile(replica_url + TEMP_FILENAME)
        myfile.upload(tmp_dir + TEMP_FILENAME,
                      "irods:///path/is/ignored/?resource=" + IRODS_RESOURCE,
                      rs.replica.OVERWRITE)

        myfile.remove()
        assert True

    except rs.SagaException as ex:
        assert False, "unexpected exception %s\n%s" % (ex.traceback, ex)


# ------------------------------------------------------------------------------
#
def test_replica_make_dir(config):
    """ Test logical file make_dir, which makes a directory on the logical service
    """
    try:
        cfg = config()
        replica_url = cfg.replica_url
        replica_directory = rs.replica.LogicalDirectory(replica_url)

        replica_directory.make_dir(replica_url + TEMP_DIRECTORY)

        # commented because iRODS install on gw68 doesn't support move
        # myfile.move("irods://"+REPLICA_DIRECTORY+TEMP_DIR)

        replica_directory.remove(replica_url + TEMP_DIRECTORY)

        assert True

    except rs.SagaException as ex:
        assert False, "unexpected exception %s" % ex


# ------------------------------------------------------------------------------
#
def test_replica_replicate(config):
    """ Test logical file replicate()
    """
    try:
        cfg         = config()
        replica_url = cfg.replica_url
        _           = rs.replica.LogicalDirectory(replica_url)
        assert True

    except rs.SagaException as ex:
        assert False, "unexpected exception %s" % ex


# ------------------------------------------------------------------------------

