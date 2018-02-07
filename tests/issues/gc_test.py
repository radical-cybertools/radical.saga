#!/usr/bin/env python

import radical,saga.attributes as sa
import time
import resource

class T (sa.Attributes) : 
    def __init__ (self) :
        self._attributes_allow_private (True)
        self._attributes_extensible    (True)
        self._attributes_camelcasing   (True)
        self._attributes_register   ('test', -1, sa.INT, sa.SCALAR, sa.WRITEABLE)
        self._attributes_set_setter ('test', self.set_test)
        self._attributes_set_getter ('test', self.get_test)

    def set_test (self, val) :
        self.test = val

    def get_test (self) :
        return self.test

for i in range (1000000) :
    t = T ()
    t.test = i
    assert (i == t.test)
    if not i % 25000 :
        mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print "%5d  %d" % (i, mem)

mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print "%5d  %d" % (i, mem)

