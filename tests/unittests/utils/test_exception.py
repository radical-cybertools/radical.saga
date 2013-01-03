
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Unit tests for saga.utils.exception.py
"""

from saga.utils.exception import *

def test_ExceptionBase():
    """ Test if ExceptionBase works properly
    """
    try:
        raise ExceptionBase('message')
        assert False
    except ExceptionBase, eb:
        if str(eb) != 'message':
            assert False
        else:
            assert str(eb) == 'message'
            assert True

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

