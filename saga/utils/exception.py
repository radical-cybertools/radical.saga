# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides exception handling utilities and base classes.
"""

import traceback

def get_traceback () :
    """ Returns the current tracback as string.
    """
    import traceback, StringIO
    output = StringIO.StringIO()
    traceback.print_exc (file=output)
    ret = output.getvalue ()
    output.close ()
    return ret

class ExceptionBase(Exception):
    """ Base exception class. 
    """
    def __init__(self, message):
        Exception.__init__(self, message)
        self._traceback = get_traceback()

    def get_traceback (self) :
        """ Return the full traceback for this exception.
        """
        return self._traceback

    traceback  = property (get_traceback) 

