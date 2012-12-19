# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA exception classes
"""

from saga.utils.exception  import ExceptionBase

class SagaException(ExceptionBase):
    """ The SAGA base exception class. All other SAGA exceptions inherit from 
        this base class and can hence be caught via it. For example::

          try:
              raise NotImplemented('Sorry, not implemented')
          except SagaException, ex:
              print ex
              print ex.traceback
    """

    def __init__  (self, message, object=None) :
        """ Create a new exception object.

            :param message: The exception message.
            :param object: The object that has caused the exception.
        """
        ExceptionBase.__init__(self, message)

        self._message   = message
        self._object    = object

    def get_message (self) :
        """ Return the exception message as a string.
        """
        return self._message

    def get_object (self) :
        """ Return the object that raised this exception.
        """
        return self._object

    def get_all_exceptions (self) :
        return [] # FIXME

    def get_all_messages (self) :
        return [] # FIXME

    def __str__ (self) :
        return self._message

    message    = property (get_message)         # string
    object     = property (get_object)          # object type
    exceptions = property (get_all_exceptions)  # list [Exception]
    messages   = property (get_all_messages)    # list [string]

class NotImplemented(SagaException):
    """ The NotImplemented exception is raised when...
        
    """
    pass

class IncorrectURL(SagaException): 
    """ The IncorrectURL exception is raised when...
        
    """
    pass

class BadParameter(SagaException): 
    """ The BadParameter exception is raised when...
        
    """
    pass

class AlreadyExists(SagaException): 
    """ The AlreadyExists exception is raised when...
        

    """
    pass

class DoesNotExist(SagaException):
    """ The DoesNotExist exception is raised when...
        
    """
    pass

class IncorrectState(SagaException): 
    """ The IncorrectState exception is raised when...
        
    """
    pass

class PermissionDenied(SagaException): 
    """ The PermissionDenied exception is raised when...
        
    """
    pass

class AuthorizationFailed(SagaException): 
    """ The AuthorizationFailed exception is raised when...
        
    """
    pass

class AuthenticationFailed(SagaException): 
    """ The AuthenticationFailed exception is raised when...
        
    """
    pass

class Timeout(SagaException): 
    """ The Timeout exception is raised when...
        
    """
    pass

class NoSuccess(SagaException): 
    """ The NoSuccess exception is raised when...
        
    """
    pass

