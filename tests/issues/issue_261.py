
import saga
import os

remote_tmp = saga.filesystem.Directory ("ssh://india.futuregrid.org/tmp/copy_test", saga.filesystem.CREATE_PARENTS)
print "initial:"
for u in remote_tmp.list () :
    print str(u)

local_dir  = saga.filesystem.Directory ("file://localhost/etc/")
local_dir.copy ('host*', "ssh://india.futuregrid.org/tmp/copy_test")
print "intermediate:"
for u in remote_tmp.list () :
    print str(u)

local_tmp = saga.filesystem.Directory ("file://localhost/tmp/copy_test", saga.filesystem.CREATE_PARENTS)
remote_tmp.copy ('host*', "file://localhost/tmp/copy_test")
print "final:"
for u in local_tmp.list () :
    print str(u)

