#!/usr/bin/env python

import radical.saga as rs


class MyContextA (rs.Context) :

    def __init__ (self, ctype) :

        self._apitype  = 'rs.Context'
        super (MyContextA, self).__init__ (ctype)


class MyContextB (rs.Context) :

    def __init__ (self, ctype) :

        self._apitype  = 'rs.Context'
        rs.Context.__init__ (self, ctype)


cs = rs.Context ('ssh') ; print("saga: %s" % cs)
ca = MyContextA ('ssh') ; print("mc a: %s" % ca)
cb = MyContextB ('ssh') ; print("mc b: %s" % cb)

