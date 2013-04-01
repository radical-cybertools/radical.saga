#!/usr/bin/env python

'''This examples shows how to use the saga.Filesystem API
   with the SFTP file adaptor.

   If something doesn't work as expected, try to set
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.
'''

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2011-2013, The SAGA Project"
__license__   = "MIT"

import sys
import saga
import getpass


def main():

    try:
        # Your ssh identity on the remote machine.
        ctx = saga.Context("ssh")
        ctx.user_id = getpass.getuser()  # Change if necessary

        session = saga.Session()
        session.add_context(ctx)

        # open home directory on a remote machine
        remote_dir = saga.filesystem.Directory('sftp://india.futuregrid.org/etc/',
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
