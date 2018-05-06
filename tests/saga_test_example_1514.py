#!/usr/bin/env python

__author__    = "Georgios Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import saga.filesystem as sfs

# we  try to copy a file
#   /tmp/foo     -> /tmp/tgt/foo
# and a dir
#   /tmp/bar/baz -> /tmp/tgt/bar/baz
#
os.system("echo 'Hello World!' > /tmp/foo")
os.system("mkdir -p              /tmp/bar")
os.system("echo 'Hello World!' > /tmp/bar/baz")

src_file = sfs.File('file:///tmp/foo')
src_dir  = sfs.File('file:///tmp/bar')

src_file.copy('file:///tmp/tgt/', sfs.CREATE_PARENTS)
src_dir.copy ('file:///tmp/tgt/', sfs.CREATE_PARENTS | sfs.RECURSIVE)

assert(os.path.isdir ('/tmp/tgt'))
assert(os.path.isfile('/tmp/tgt/foo'))
assert(os.path.isdir ('/tmp/tgt/bar'))
assert(os.path.isfile('/tmp/tgt/bar/baz'))


