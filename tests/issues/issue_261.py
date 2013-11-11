
import saga
import os

local_dir = saga.filesystem.Directory ("file://localhost/etc/")
local_dir.copy ('host*', "ssh://localhost/tmp/stage_test/")

check = os.popen ("ls -la /tmp/stage_test/")
for line in check.readlines() :
    print line[:-1]

