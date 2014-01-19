
import saga

class MyContextA (saga.Context) :

    def __init__ (self, ctype) :

        self._apitype  = 'saga.Context'
        super (MyContextA, self).__init__ (ctype)


class MyContextB (saga.Context) :

    def __init__ (self, ctype) :

        self._apitype  = 'saga.Context'
        saga.Context.__init__ (self, ctype)


cs = saga.Context ('ssh') ; print "saga: %s" % cs
ca = MyContextA   ('ssh') ; print "mc a: %s" % ca
cb = MyContextB   ('ssh') ; print "mc b: %s" % cb

