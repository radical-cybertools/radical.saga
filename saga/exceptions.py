# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' SAGA Exception Class.
'''

from saga.utils.exception  import ExceptionBase

class SagaException(ExceptionBase):
    pass

class NotImplemented(SagaException):
    pass


