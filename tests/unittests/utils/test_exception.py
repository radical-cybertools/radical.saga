
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for saga.utils.exception.py
"""

import saga

import radical.utils as ru


def test_Exceptions():
    """ Test if Exceptions work properly
    """
    try:
        msg = 'this is the message'
        raise saga.SagaException(msg)
        assert False
    except saga.SagaException, se:
        assert (msg in str(se)), "'%s' not in '%s'" % (msg, se)



