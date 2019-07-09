
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

    cfg = config()

    replica_url = cfg.replica_url

    with open(TEMP_FILENAME, "wb") as f:
        f.write("x" * (FILE_SIZE * pow(2,20)) )

    # clear old file
    out, err, ret = ru.sh_callout(["irm", TEMP_FILENAME])

    d = rs.replica.LogicalDirectory(replica_url)
    f = rs.replica.LogicalFile(replica_url + TEMP_FILENAME)
    f.upload(home_dir + TEMP_FILENAME, \
                      "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE)

    #myfile.upload(home_dir + TEMP_FILENAME, \
    #                  "irods:///this/path/is/ignored")

    print "Deleting file locally : %s" % (home_dir + TEMP_FILENAME)
    os.remove(home_dir + TEMP_FILENAME)

    print "Downloading logical file %s to current/default directory" % \
        (replica_url + TEMP_FILENAME)
    myfile.download(TEMP_FILENAME)

    print "Deleting downloaded file locally : %s" % (home_dir + TEMP_FILENAME)
    os.remove(home_dir + TEMP_FILENAME)


# ------------------------------------------------------------------------------
#
def test_replica_get_size(config):
    """ Test logical file get_size()
    """
    try:
        cfg = config()
        the_url = cfg.job_service_url # from test config file
        the_session = cfg.session # from test config file
        replica_url = cfg.replica_url
        replica_directory = rs.replica.LogicalDirectory(replica_url)

        home_dir = os.path.expanduser("~"+"/")
        print "Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir+TEMP_FILENAME)

        # create a file for us to use
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write("x" * (FILE_SIZE * pow(2,20)) )

        print "Creating logical directory object"
        mydir = rs.replica.LogicalDirectory(replica_url)

        print "Uploading file to check size"
        myfile = rs.replica.LogicalFile(replica_url+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
                          "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE,
                          rs.replica.OVERWRITE)

        print "Checking size"
        myfile = rs.replica.LogicalFile(replica_url+TEMP_FILENAME)
        print myfile.get_size()

        assert True

    except rs.SagaException as ex:
#        print ex.traceback
        assert False, "unexpected exception %s\n%s" % (ex.traceback, ex)

# ------------------------------------------------------------------------------
#
def test_replica_remove(config):
    """ Test logical file remove, which should remove the file from the remote resource
    """
    try:
        cfg = config()
        the_url = cfg.job_service_url # from test config file
        the_session = cfg.session # from test config file
        replica_url = cfg.replica_url
        replica_directory = rs.replica.LogicalDirectory(replica_url)

        home_dir = os.path.expanduser("~"+"/")
        print "Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir+TEMP_FILENAME)

        # create a file for us to use
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write("x" * (FILE_SIZE * pow(2,20)) )

        print "Creating logical directory object"
        mydir = rs.replica.LogicalDirectory(replica_url)

        print "Uploading temporary"
        myfile = rs.replica.LogicalFile(replica_url+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
                          "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE,
                          rs.replica.OVERWRITE)

        print "Removing temporary file."
        myfile.remove()
        assert True

    except rs.SagaException as ex:
#        print ex.traceback
        assert False, "unexpected exception %s\n%s" % (ex.traceback, ex)


# ------------------------------------------------------------------------------
#
def test_replica_make_dir(config):
    """ Test logical file make_dir, which makes a directory on the logical service
    """
    try:
        cfg = config()
        the_url = cfg.job_service_url # from test config file
        the_session = cfg.session # from test config file
        replica_url = cfg.replica_url
        replica_directory = rs.replica.LogicalDirectory(replica_url)

        print "Making test dir %s on " % (replica_url+TEMP_DIRECTORY)
        replica_directory.make_dir(replica_url+TEMP_DIRECTORY)

        #commented because iRODS install on gw68 doesn't support move
        #print "Moving file to %s test dir on iRODS" % (REPLICA_DIRECTORY+TEMP_DIR)
        #myfile.move("irods://"+REPLICA_DIRECTORY+TEMP_DIR)

        print "Deleting test dir %s from " % (replica_url+TEMP_DIRECTORY)
        replica_directory.remove(replica_url+TEMP_DIRECTORY)

        assert True

    except rs.SagaException as ex:
        assert False, "unexpected exception %s" % ex

# ------------------------------------------------------------------------------
#
def test_replica_replicate(config):
    """ Test logical file replicate()
    """
    try:
        cfg = config()
        the_url = cfg.job_service_url # from test config file
        the_session = cfg.session # from test config file
        replica_url = cfg.replica_url
        replica_directory = rs.replica.LogicalDirectory(replica_url)
        assert True

    except rs.SagaException as ex:
        assert False, "unexpected exception %s" % ex
