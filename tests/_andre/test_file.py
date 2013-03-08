
import os
import saga

os.system ("mkdir /tmp/src ; ls -la /tmp > /tmp/src/src.dat")

d = saga.filesystem.Directory ("file://localhost/tmp/src/")

f = saga.filesystem.File("file://localhost/etc/passwd")
print f.size
f.copy('/tmp/')

print "copy entry from dir"
d.copy ("src.dat", "tgt.dat")

print "copy self from dir"
d.copy ("/tmp/tgt", flags=saga.filesystem.RECURSIVE)

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

print "move entry"
print f_tgt.move ('TGT.DAT')

print "inspect moved entry"
print f_tgt.url

print "remove entry"
f_src.remove ()

print "remove from directory"
d.remove ("bak.dat")

print "list"
for name in d.list () :
  print name
