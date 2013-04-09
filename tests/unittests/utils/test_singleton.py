
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for saga.utils.singleton.py
"""

from saga.utils.singleton import *

class _MyClass():
    __metaclass__ = Singleton


def test_Singleton():
    """ Test if singleton instances are identical
    """ 
    assert _MyClass() == _MyClass()

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

