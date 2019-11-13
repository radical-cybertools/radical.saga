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

import radical.saga as rs


def main():

    try:
        ctx = rs.Context("x509")
        ctx.user_proxy = '/Users/mark/proj/myproxy/xsede.x509'

        session = rs.Session()
        session.add_context(ctx)

        # open home directory on a remote machine
        #remote_dir = rs.filesystem.Directory('sftp://hotel.futuregrid.org/opt/',
        #remote_dir = rs.filesystem.Directory('go://netbook/', session=session)
        #remote_dir = rs.filesystem.Directory('go://marksant#netbook/~/tmp/go')
        remote_dir = rs.filesystem.Directory('go://xsede#stampede/~/tmp/go/')

        for entry in remote_dir.list():
            if remote_dir.is_dir(entry):
                print("d %12s %s" % (remote_dir.get_size(entry), entry))
            elif remote_dir.is_link(entry):
                print("l %12s %s" % (remote_dir.get_size(entry), entry))
            elif remote_dir.is_file(entry):
                print("- %12s %s" % (remote_dir.get_size(entry), entry))
            else:
                print('Other taste ....: %s' % entry)

        return 0

    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1

if __name__ == "__main__":
    sys.exit(main())
