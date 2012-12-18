# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
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
