
import saga
import os

remote_dir = saga.filesystem.Directory ("ssh://india.futuregrid.org/tmp/copy_test", saga.filesystem.CREATE_PARENTS)
print "initial: %s" % str(remote_dir.list ())

local_dir  = saga.filesystem.Directory ("file://localhost/etc/")
local_dir.copy ('host*', "ssh://india.futuregrid.org/tmp/copy_test")
print "final:"
for u in remote_dir.list () :
    print str(u)


