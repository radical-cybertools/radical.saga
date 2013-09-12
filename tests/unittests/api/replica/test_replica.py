
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import time
import saga
import saga.utils.test_config as sutc
import os

from copy import deepcopy

FILE_SIZE = 1 # in megs, approx                                                                                                         
NUM_REPLICAS = 5 # num replicas to create
TEMP_FILENAME = "temp.txt"
IRODS_RESOURCE = "osgGridFtpGroup"

# ------------------------------------------------------------------------------
#
def test_replica_entry():
    """ Test logical file entry 
    """
    try:
        tc = sutc.TestConfig()
        the_url = tc.js_url # from test config file
        the_session = tc.session # from test config file
        irods_url = tc.replica_url
        print "Testing iRODS entry with URL: %s" % irods_url
        print " *** NOTE: only localhost is supported, which makes use "\
              "of the current iRODS installion/environment variables."
        irods_directory = saga.replica.LogicalDirectory(irods_url)
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
        irods_url = tc.replica_url
        irods_directory = saga.replica.LogicalDirectory(irods_url)
        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex

# ------------------------------------------------------------------------------
#
def test_replica_directory_listing():
    """ Test logical file directory listing
    """
    try:
        tc = sutc.TestConfig()
        the_url = tc.js_url # from test config file
        the_session = tc.session # from test config file
        irods_url = tc.replica_url
        irods_directory = saga.replica.LogicalDirectory(irods_url)
        
        for entry in irods_directory.list():
            print entry

        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex


# ------------------------------------------------------------------------------
#
def test_upload_and_download():
    """ Test iRODS file upload and download 
    """
    try:
        tc = sutc.TestConfig()
        home_dir = os.path.expanduser("~"+"/")
        irods_url = tc.replica_url
        print "Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir+TEMP_FILENAME)

        # create a file for us to use with iRODS                                                                                        
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write ("x" * (FILE_SIZE * pow(2,20)) )

        print "Creating iRODS directory object"
        mydir = saga.replica.LogicalDirectory(irods_url)

        print "Making sure there is no file existing remotely"
        import subprocess
        subprocess.call(["irm", TEMP_FILENAME])

        print "Uploading file to iRODS: %s" %  home_dir+TEMP_FILENAME
        myfile = saga.replica.LogicalFile(irods_url+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
                          "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE)

        #myfile.upload(home_dir + TEMP_FILENAME, \
        #                  "irods:///this/path/is/ignored")

        print "Deleting file locally : %s" % (home_dir + TEMP_FILENAME)                                                   
        os.remove(home_dir + TEMP_FILENAME)
        assert True

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex

