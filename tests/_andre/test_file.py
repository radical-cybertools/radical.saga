
import saga

d = saga.filesystem.Directory ("file://localhost/tmp/src/")

print "copy entry from dir"
d.copy ("src.dat", "tgt.dat")

print "copy self from dir"
d.copy ("/tmp/tgt")

f_tgt = d.open ('tgt.dat')
f_src = d.open ('src.dat')

print "copy self from entry"
f_tgt.copy ('bak.dat')

print "size from dir"
print d.get_size ('src.dat')

print "size from entry"
print f_src.size

print "size from entry"
print f_tgt.size

print "list"
print d.list ()


