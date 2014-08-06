
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import time
import saga
import os

from copy import deepcopy

import radical.utils.testing as testing


FILE_SIZE      = 1 # in megs, approx                                                                                                         
NUM_REPLICAS   = 5 # num replicas to create
TEMP_FILENAME  = "temp.txt"
TEMP_DIRECTORY = "foozlefuzz"
IRODS_RESOURCE = "osgGridFtpGroup"

#        print "Testing iRODS entry with URL: %s" % replica_url
#        print " *** NOTE: only localhost is supported, which makes use "\
#              "of the current iRODS installion/environment variables."


# ------------------------------------------------------------------------------
#
def test_replica_entry():
    """ Test logical file entry 
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)
        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex

# ------------------------------------------------------------------------------
#
def test_replica_directory():
    """ Test logical file directory 
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)
        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex

# ------------------------------------------------------------------------------
#
def test_replica_directory_listing():
    """ Test logical file directory listing
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)

        print "Printing directory listing: please verify."\
              "\n%s" % replica_url 
        
        for entry in replica_directory.list():
            print entry

        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex


# ------------------------------------------------------------------------------
#
def test_upload_and_download():
    """ Test file upload and download"
    """
    try:
        tc = testing.get_test_config ()
        home_dir = os.path.expanduser("~"+"/")
        replica_url = tc.replica_url
        print "Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir+TEMP_FILENAME)

        # create a file for us to use with iRODS                                                                                        
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write ("x" * (FILE_SIZE * pow(2,20)) )

        print "Creating logical directory object"
        mydir = saga.replica.LogicalDirectory(replica_url)

        print "Making sure there is no file existing remotely"
        import subprocess
        subprocess.call(["irm", TEMP_FILENAME])

        print "Uploading file to remote host: %s" %  home_dir+TEMP_FILENAME
        myfile = saga.replica.LogicalFile(replica_url+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
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
        
        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex


# ------------------------------------------------------------------------------
#
def test_replica_get_size():
    """ Test logical file get_size()
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)

        home_dir = os.path.expanduser("~"+"/")
        print "Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir+TEMP_FILENAME)

        # create a file for us to use
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write ("x" * (FILE_SIZE * pow(2,20)) )

        print "Creating logical directory object"
        mydir = saga.replica.LogicalDirectory(replica_url)

        print "Uploading file to check size"
        myfile = saga.replica.LogicalFile(replica_url+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
                          "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE, saga.replica.OVERWRITE)

        print "Checking size"
        myfile = saga.replica.LogicalFile(replica_url+TEMP_FILENAME)
        print myfile.get_size()

        assert True

    except saga.SagaException as ex:
#        print ex.traceback
        assert False, "unexpected exception %s\n%s" % (ex.traceback, ex)

# ------------------------------------------------------------------------------
#
def test_replica_remove():
    """ Test logical file remove, which should remove the file from the remote resource
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)

        home_dir = os.path.expanduser("~"+"/")
        print "Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir+TEMP_FILENAME)

        # create a file for us to use
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write ("x" * (FILE_SIZE * pow(2,20)) )

        print "Creating logical directory object"
        mydir = saga.replica.LogicalDirectory(replica_url)

        print "Uploading temporary"
        myfile = saga.replica.LogicalFile(replica_url+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
                          "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE, saga.replica.OVERWRITE)

        print "Removing temporary file."
        myfile.remove()
        assert True

    except saga.SagaException as ex:
#        print ex.traceback
        assert False, "unexpected exception %s\n%s" % (ex.traceback, ex)


# ------------------------------------------------------------------------------
#
def test_replica_make_dir():
    """ Test logical file make_dir, which makes a directory on the logical service
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)

        print "Making test dir %s on " % (replica_url+TEMP_DIRECTORY)
        replica_directory.make_dir(replica_url+TEMP_DIRECTORY)

        #commented because iRODS install on gw68 doesn't support move                                                                   
        #print "Moving file to %s test dir on iRODS" % (REPLICA_DIRECTORY+TEMP_DIR)                                                       
        #myfile.move("irods://"+REPLICA_DIRECTORY+TEMP_DIR)                                                                               

        print "Deleting test dir %s from " % (replica_url+TEMP_DIRECTORY)
        replica_directory.remove(replica_url+TEMP_DIRECTORY)

        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex

# ------------------------------------------------------------------------------
#
def test_replica_replicate():
    """ Test logical file replicate()
    """
    try:
        tc = testing.get_test_config ()
        the_url = tc.job_service_url # from test config file
        the_session = tc.session # from test config file
        replica_url = tc.replica_url
        replica_directory = saga.replica.LogicalDirectory(replica_url)
        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex
