#!/usr/bin/env python

'''This examples shows how to use the saga.Filesystem API
   with the HTTP file adaptor.

   If something doesn't work as expected, try to set
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.
'''

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2011-2013, The SAGA Project"
__license__   = "MIT"

import sys
import saga


def main():

    try:
        # open file on a remote web server - WARNING: size is ~ 800 MB
        remote_file = saga.filesystem.File('http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit')

        # copy the remote file to /tmp/ on the local machine
        remote_file.copy('file://localhost/tmp/', flags=saga.filesystem.OVERWRITE)
        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
