
from __future__ import absolute_import
from __future__ import print_function
from six.moves import range
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import sys
import saga

os.system ("ls -la /tmp > /tmp/tmp.txt")

f = saga.filesystem.File ("go://gridftp.stampede.tacc.xsede.org/~/.bashrc")
f.copy ("go://gridftp.stampede.tacc.utexs.edu/~/b")

sys.exit()

host = "gw68.quarry.iu.teragrid.org"
src  = saga.Url('sftp://%s/etc/passwd' % host)
tgt  = saga.Url('file://localhost/tmp/')
f    = saga.filesystem.File (src)
f.copy (tgt)

def test_tests (url) :

    e = saga.namespace.Entry     (url)
    print()
    print("ns_entry  : %s" % e.url)
    print("  is_dir  : %s" % e.is_dir ())
    print("  is_link : %s" % e.is_link ())
    print("  is_entry: %s" % e.is_entry ())
    
    if e.is_dir () :
        e = saga.namespace.Directory (url)
        print()
        print("ns_dir    : %s" % e.url)
        print("  is_dir  : %s" % e.is_dir ())
        print("  is_link : %s" % e.is_link ())
        print("  is_entry: %s" % e.is_entry ())
    
    e = saga.filesystem.File      (url)
    print()
    print("fs_entry  : %s" % e.url)
    print("  is_dir  : %s" % e.is_dir ())
    print("  is_link : %s" % e.is_link ())
    print("  is_file : %s" % e.is_file ())
    print("  is_entry: %s" % e.is_entry ())
    print("  size    : %s" % e.get_size ())

    if e.is_dir () :
        e = saga.filesystem.Directory (url)
        print()
        print("fs_dir    : %s" % e.url)
        print("  is_dir  : %s" % e.is_dir ())
        print("  is_link : %s" % e.is_link ())
        print("  is_file : %s" % e.is_file ())
        print("  is_entry: %s" % e.is_entry ())
        print("  size    : %s" % e.get_size ())
    
test_tests ("sftp://localhost/tmp/src/")
# test_tests ("file://localhost/tmp/src/")
# test_tests ("file://localhost/tmp/src/src.dat")
# test_tests ("file://localhost/tmp/src/src.lnk")

d = saga.filesystem.Directory ("ssh://localhost/tmp/src/")
f = saga.filesystem.File("ssh://localhost/etc/passwd")
print(f.size)
f.copy('/tmp/')

print("copy entry from dir")
d.copy ("src.dat", "tgt.dat")

print("copy self from dir")
d.copy ("/tmp/tgt", flags=saga.filesystem.RECURSIVE)

f_tgt = d.open ('tgt.dat')
f_src = d.open ('src.dat')

print("copy self from entry")
f_tgt.copy ('bak.dat')

print("size from dir")
print(d.get_size ('src.dat'))

print("size from entry")
print(f_src.size)

print("size from entry")
print(f_tgt.size)

print("move entry")
print(f_tgt.move ('TGT.DAT'))

print("inspect moved entry")
print(f_tgt.url)

print("remove entry")
f_src.remove ()

print("remove from directory")
d.remove ("bak.dat")

print("list")
for name in d.list () :
  print(name)

print("many remote files")
srcdir = saga.filesystem.Directory ("ssh://india.futuregrid.org/tmp/andre.merzky/src/", 
                                    saga.filesystem.CREATE_PARENTS)

tgtdir = saga.filesystem.Directory ("file://localhost/tmp/andre.merzky/tgt/", 
                                    saga.filesystem.CREATE_PARENTS)

files  = []
for i in range (0, 500) :
    f = srcdir.open ("test_%02d.dat" % i, saga.filesystem.CREATE)
    print("copy %s file://localhost/tmp/andre.merzky/tgt/" % f.url)
    f.copy  ("file://localhost/tmp/andre.merzky/tgt/")
    f.close ()

