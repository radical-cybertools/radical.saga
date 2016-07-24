
from __future__ import absolute_import
import saga

c = saga.Context ('ssh')
c.user_id = 'dinesh'

s = saga.Session ()
s.add_context (c)

js = saga.job.Service("lsf+ssh://yellowstone.ucar.edu", session=s)

