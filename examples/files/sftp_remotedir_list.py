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

        for entry in remote_dir.list():
            if remote_dir.is_dir(entry):
                print "d %12s %s" % (remote_dir.get_size(entry), entry)
            else:
                print "- %12s %s" % (remote_dir.get_size(entry), entry)



    except saga.SagaException, ex:
        print "An error occured during file operation: %s" % (str(ex))
        sys.exit(-1)

if __name__ == "__main__":
    main()
