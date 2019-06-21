#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for saga.utils.exception.py
"""

import radical.saga as saga


# ------------------------------------------------------------------------------
#
def test_Exceptions():
    """ Test if Exceptions work properly
    """
    try:
        msg = 'this is the message'
        raise saga.SagaException(msg)
        assert False
    except saga.SagaException as se:
        assert (msg in str(se)), "'%s' not in '%s'" % (msg, se)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_Exceptions()


# ------------------------------------------------------------------------------

