#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys

import radical.saga as rs


def main():

    try:

        c = rs.Context ('ssh')
        c.user_id   = 'tg12736'
        c.user_cert = '/home/user/ssh/id_rsa_xsede' # private key derived from cert

        s = rs.Session (default=False)            # create session with no contexts
        s.add_context (c)

        js = rs.job.Service ('ssh://login1.stampede.tacc.utexas.edu', session=s)
        js.run_job ("/bin/true")

    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1

if __name__ == "__main__":
    sys.exit(main())
