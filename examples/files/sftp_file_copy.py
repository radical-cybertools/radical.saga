#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

'''This examples shows how to use the saga.Filesystem API

   If something doesn't work as expected, try to set 
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.

   If you think you have encountered a defect, please 
   report it at: https://github.com/saga-project/saga-python/issues
'''

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2011-2013, The SAGA Project"
__license__   = "MIT"

import sys
import saga

def main():
    
    try: 
        # open home directory on a remote machine
        remote_dir = saga.filesystem.Directory('sftp://india.futuregrid.org/etc/')
        # Alternatively: 
        # Use custom session to create Directory object
        #remote_dir = saga.filesystem.Directory('sftp://queenbee.loni.org/etc/', 
        #                                  session=session)

        # copy .bash_history to /tmp/ on the local machine
        remote_dir.copy('hosts', 'file://localhost/tmp/') 

        # list 'h*' in local /tmp/ directory
        local_dir = saga.filesystem.Directory('file://localhost/tmp/')
        print local_dir.list(pattern='h*')

    except saga.SagaException, ex:
        print "An error occured during file operation: %s" % (str(ex))
        sys.exit(-1)

if __name__ == "__main__":
    main()
