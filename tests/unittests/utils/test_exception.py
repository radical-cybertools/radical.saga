# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Unit tests for saga.utils.exception.py
'''

from saga.utils.exception import *

def test_ExceptionBase():
    try:
        raise ExceptionBase('message')
        assert False
    except ExceptionBase, eb:
        if str(eb) != 'message':
            assert False
        else:
            assert str(eb) == 'message'
            assert True
