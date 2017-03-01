#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


'''This examples shows how to use the saga.Filesystem API
   with the SFTP file adaptor.

   If something doesn't work as expected, try to set
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.

   If you think you have encountered a defect, please 
   report it at: https://github.com/saga-project/saga-python/issues
'''

import sys
import saga
import getpass


def main():

    try:
        # Your ssh identity on the remote machine.
        ctx = saga.Context("ssh")
       #ctx.user_id = getpass.getuser()  # Change if necessary

        session = saga.Session()
        session.add_context(ctx)

        # open home directory on a remote machine
        remote_dir = saga.filesystem.Directory('sftp://stampede.tacc.xsede.org/tmp/',
                                               session=session)

        for entry in remote_dir.list():
            if remote_dir.is_dir(entry):
                print "d %12s %s" % (remote_dir.get_size(entry), entry)
            else:
                print "- %12s %s" % (remote_dir.get_size(entry), entry)
        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
