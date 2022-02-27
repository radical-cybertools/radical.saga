#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os

import radical.saga as rs


# ------------------------------------------------------------------------------
#

c = rs.Context ('ssh')
c.user_id   = os.environ['USER']
c.user_cert = '%s/.ssh/id_rsa' % os.environ['HOME']

s = rs.Session (default=False)  # create session with no contexts
s.add_context (c)

js = rs.job.Service ('ssh://localhost', session=s)
js.run_job ("/bin/true")


# ------------------------------------------------------------------------------

