# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' SAGA Exception Class.
'''
import traceback

from saga.utils.exception  import ExceptionBase

class SagaException(ExceptionBase):

    def __init__  (self, message, object=None) :
        self._message   = message
        self._object    = object
        self._traceback = repr (traceback.format_stack ())

    def get_message (self) :
        return self._message

    def get_object (self) :
        return self._object

    def get_traceback (self) :
        return self._traceback

    def get_all_exceptions (self) :
        return [] # FIXME

    def get_all_messages (self) :
        return [] # FIXME

    def __str__ (self) :
        return self._message


    message    = property (get_message)         # string
    object     = property (get_object)          # object type
    traceback  = property (get_traceback)       # string
    exceptions = property (get_all_exceptions)  # list [Exception]
    messages   = property (get_all_message)     # list [string]


class NotImplemented       (SagaException)   : pass
class IncorrectURL         (SagaException)   : pass
class BadParameter         (SagaException)   : pass
class AlreadyExists        (SagaException)   : pass
class DoesNotExist         (SagaException)   : pass
class IncorrectState       (SagaException)   : pass
class PermissionDenied     (SagaException)   : pass
class AuthorizationFailed  (SagaException)   : pass
class AuthenticationFailed (SagaException)   : pass
class Timeout              (SagaException)   : pass
class NoSuccess            (SagaException)   : pass

