#!/usr/bin/env python

__author__    = "Andre Merzky, Mark Santcroos"
__copyright__ = "Copyright 2015, The SAGA Project"
__license__   = "MIT"

'''This examples shows how to use the rs.Filesystem API
   with the Globus Online file adaptor.

   If something doesn't work as expected, try to set
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.

   If you think you have encountered a defect, please 
   report it at: https://github.com/radical-cybertools/radical.saga/issues
'''

import sys
import os

import radical.saga as rs


def main():

    try:
        ctx = rs.Context("x509")
        ctx.user_proxy = '/Users/mark/proj/myproxy/xsede.x509'

        session = rs.Session()
        session.add_context(ctx)

        source = "go://marksant#netbook/Users/mark/tmp/go/"
        #destination = "go://xsede#stampede/~/tmp/"
        #destination = "go://gridftp.stampede.tacc.xsede.org/~/tmp/"
        destination = "go://oasis-dm.sdsc.xsede.org/~/tmp/"
        #destination = "go://ncsa#BlueWaters/~/tmp/"
        filename = "my_file"

        # open home directory on a remote machine
        source_dir = rs.filesystem.Directory(source)

        # copy .bash_history to /tmp/ on the local machine
        source_dir.copy(filename, destination)

        # list 'm*' in local /tmp/ directory
        dest_dir = rs.filesystem.Directory(destination)
        for entry in dest_dir.list(pattern='%s*' % filename[0]):
            print(entry)

        dest_file = rs.filesystem.File(os.path.join(destination, filename))
        assert dest_file.is_file() == True
        assert dest_file.is_link() == False
        assert dest_file.is_dir() == False
        print('Size: %d' % dest_file.get_size())

        return 0

    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1

if __name__ == "__main__":
    sys.exit(main())
