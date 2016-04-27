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


def main():

    try:
        # Your ssh identity on the remote machine.
        ctx = saga.Context("ssh")

        # Change e.g., if you have a differnent username on the remote machine
        #ctx.user_id = "your_ssh_username"

        session = saga.Session()
        session.add_context(ctx)

        # open home directory on a remote machine
        remote_dir = saga.filesystem.Directory('sftp://stampede.tacc.xsede.org/etc/',
                                               session=session)

        # copy .bash_history to /tmp/ on the local machine
        remote_dir.copy('hosts', 'file://localhost/tmp/')

        # list 'h*' in local /tmp/ directory
        local_dir = saga.filesystem.Directory('file://localhost/tmp/')
        for entry in local_dir.list(pattern='h*'):
            print entry
        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
