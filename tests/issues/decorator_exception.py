import sys
import radical.saga as saga
import traceback
import radical.utils.signatures as rus

class AttribTest (saga.Attributes) :

    def __init__ (self) :

        self._attributes_extensible (False)

    @rus.takes   ('AttribTest', int)
    @rus.returns (str)
    def test (self, my_int) :
        self.test_attrib()
        return 1

at = AttribTest ()
at.test (1)
at.test ("test")
at.test (1, 2)


