from __future__ import absolute_import
import sys
import saga
import traceback
import radical.utils.signatures as rus
import six

class AttribTest (saga.Attributes) :

    def __init__ (self) :

        self._attributes_extensible (False)

    @rus.takes   ('AttribTest', int)
    @rus.returns (six.string_types)
    def test (self, my_int) :
        self.test_attrib()
        return 1

at = AttribTest ()
at.test (1)
at.test ("test")
at.test (1, 2)


