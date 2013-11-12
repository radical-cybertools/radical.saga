
import saga
import os

remote_tmp = saga.filesystem.Directory ("sftp://india.futuregrid.org/tmp/copy_test", saga.filesystem.CREATE_PARENTS)

print "initial:"
for u in remote_tmp.list () :
    print str(u)

local_dir  = saga.filesystem.Directory ("file:///etc")
local_dir.copy ('h*', "sftp://india.futuregrid.org/tmp/copy_test", saga.filesystem.RECURSIVE)
print "intermediate:"
for u in remote_tmp.list () :
    print str(u)

local_tmp = saga.filesystem.Directory ("file://localhost/tmp/copy_test", saga.filesystem.CREATE_PARENTS)
remote_tmp.copy ('h*', "file://localhost/tmp/copy_test", saga.filesystem.RECURSIVE)
print "final:"
for u in local_tmp.list () :
    print str(u)

