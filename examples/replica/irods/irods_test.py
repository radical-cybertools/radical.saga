
__author__    = "Andre Merzky, Ashley Z, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


#!/usr/bin/env python2.4
# -*- coding: utf-8 -*-


'''This example shows how to use the iRODS adaptor in 
   some basic ways.  If it executes successfully, the
   iRODS adaptor should work OK on your setup.

   If something doesn't work as expected, try to set 
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.

   If you think you have encountered a defect, please 
   report it at: https://github.com/radical-cybertools/radical.saga/issues
'''

__author__    = "Ashley Zebrowski"
__copyright__ = "Copyright 2012-2013, Ashley Zebrowski"
__license__   = "MIT"

import sys
import os
import logging
import subprocess

import radical.saga as rs


FILE_SIZE     = 1                   # in megs, approx
NUM_REPLICAS  = 1                   # num replicas to create
TEMP_FILENAME = "test.txt"          # filename to create and use for testing
TEMP_DIR      = "/irods_test_dir/"  # directory to create and use for testing


def usage():
    print('Usage: python %s ' % __file__)
    print(' <IRODS_DIRECTORY> (e.x. /osg/home/username/)')
    print(' <IRODS_RESOURCE>  (e.x. osgGridFtpGroup)>')
    print()
    print("For example: python %s " % __file__ + "/irods/mydir/ irodsGroup")
    print("Please specify an iRODS resource group for <IRODS_RESOURCE> " \
          "if possible in order to properly test replication.")


def main(args):

    if len(args) != 3:
        usage()
        exit(-1)

    # directory to store our iRODS files in, don't forget trailing and leading /
    IRODS_DIRECTORY = args[1]

    # iRODS resource or resource group to upload files to
    IRODS_RESOURCE  = args[2]

    # remove any intermediary files that may have been created on iRODS 
    # from an earlier, failed run of this script
    #
    try:
        subprocess.check_call(["irm", IRODS_DIRECTORY + TEMP_FILENAME])
    except:
        pass

    try:
        subprocess.check_call(["irm", '-r', IRODS_DIRECTORY + TEMP_DIR])
    except:
        pass

    try:
        subprocess.check_call(["rm", "/tmp/" + TEMP_FILENAME])
    except:
        pass

    try:
        myfile = rs.replica.LogicalFile('irods://' + IRODS_DIRECTORY +
                                                                  TEMP_FILENAME)

        # grab our home directory (tested on Linux)
        home_dir = os.path.expanduser("~" + "/")
        print("Creating temporary file of size %dM : %s" % \
            (FILE_SIZE, home_dir + TEMP_FILENAME))

        # create a file for us to use with iRODS
        with open(home_dir+TEMP_FILENAME, "wb") as f:
            f.write ("x" * (FILE_SIZE * pow(2,20)) )

        print("Creating iRODS directory object")
        mydir = rs.replica.LogicalDirectory("irods://localhost/" + IRODS_DIRECTORY) 

        print("Uploading file to iRODS")
        myfile = rs.replica.LogicalFile('irods://localhost/'+IRODS_DIRECTORY+TEMP_FILENAME)
        myfile.upload(home_dir + TEMP_FILENAME, \
                     "irods:///this/path/is/ignored/?resource="+IRODS_RESOURCE)

        print("Deleting file locally : %s" % (home_dir + TEMP_FILENAME))
        os.remove(home_dir + TEMP_FILENAME)

        print("Printing iRODS directory listing for %s " % ("irods://localhost/" + IRODS_DIRECTORY))
        for entry in mydir.list():
            print(entry)

        print("Creating iRODS file object")
        myfile = rs.replica.LogicalFile('irods://localhost/' + IRODS_DIRECTORY+TEMP_FILENAME)
        
        print("Creating",NUM_REPLICAS,"replicas for",IRODS_DIRECTORY+TEMP_FILENAME)
        for i in range(NUM_REPLICAS):
            myfile.replicate("irods://localhost/this/path/is/ignored/?resource="+IRODS_RESOURCE)

        print("Locations the file is stored at on iRODS:")
        for entry in myfile.list_locations():
            print(entry)

        print("Downloading logical file %s to /tmp/" % \
            (IRODS_DIRECTORY + TEMP_FILENAME)) 
        myfile.download("/tmp/")

        print("Deleting downloaded file locally: %s" % ("/tmp" + TEMP_FILENAME))
        os.remove("/tmp/" + TEMP_FILENAME)

        print("Making test dir %s on iRODS" % (IRODS_DIRECTORY+TEMP_DIR))
        mydir.make_dir("irods://localhost/"+IRODS_DIRECTORY+TEMP_DIR)
        
        print("Deleting test dir %s from iRODS" % (IRODS_DIRECTORY+TEMP_DIR))
        mydir.remove("irods://localhost/"+IRODS_DIRECTORY+TEMP_DIR)

        print("Deleting file %s from iRODS" % (IRODS_DIRECTORY+TEMP_FILENAME))
        myfile.remove()

    except Exception as ex:
        logging.exception("An error occured while executing the test script!"
                          "Please run with SAGA_VERBOSE=4 set in the"
                          "environment for debug output.\n  %s"
                          % (str(ex)))
        sys.exit(-1)

    print("iRODS test script finished execution")

if __name__ == "__main__":
    sys.exit(main(sys.argv))
