
import saga

d = saga.filesystem.Directory ("file://localhost/tmp/src/")

print "copy entry"
d.copy ("src.dat", "tgt.dat")

print "copy self"
d.copy ("/tmp/tgt")

print "list"
print d.list ()
