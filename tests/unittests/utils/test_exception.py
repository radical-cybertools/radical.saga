
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
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
        assert (str(eb) == 'ExceptionBase: message'), "'%s' != '%s'" % (str(eb), 'message')

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

