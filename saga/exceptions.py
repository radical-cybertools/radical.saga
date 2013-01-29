# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA exception classes """


import operator

from saga.utils.exception  import ExceptionBase, get_traceback


class SagaException(ExceptionBase):
    """ The SAGA base exception class. All other SAGA exceptions inherit from 
        this base class and can hence be caught via it. For example::

          try:
              raise NotImplemented('Sorry, not implemented')
          except SagaException, ex:
              print ex
              print ex.traceback
    """

    def __init__  (self, message, api_object=None) :
        """ Create a new exception object.

            :param message: The exception message.
            :param object: The object that has caused the exception.
        """
        ExceptionBase.__init__ (self, message)

        self._message       = message
        self._object        = api_object
        self._messages      = [message]
        self._exceptions    = [self]
        self._top_exception = self
        self._traceback     = get_traceback (1)


    def _clone (self) :

        clone = self.__class__ (self._message, self._object)
        clone._messages  = self._messages
        clone._exception = self._exceptions
        clone._traceback = self._traceback

        return clone


    def get_message (self) :
        """ Return the exception message as a string.
        """
        return self._message


    def get_object (self) :
        """ Return the object that raised this exception.
        """
        return self._object


    def _add_exception (self, e) :
        self._exceptions.append (e)
        self._messages.append   (e.message)

        if e._rank > self._top_exception._rank :
            self._top_exception = e


    def _get_exception_stack (self) :

        if self._top_exception == self :
            return self
        
        # damned, we can't simply recast ourself to the top exception type -- so
        # we have to create a new exception with that type, and copy all state
        # over...

        # create a new exception with same type as top_exception
        clone = self._top_exception._clone ()
        clone._exceptions = []
        clone._messages   = []

        # copy all state over
        for e in sorted (self._exceptions, key=operator.attrgetter ('_rank'), reverse=True) :
            clone._exceptions.append (e)
            clone._messages.append (e._message)

        return clone

                
    def get_all_exceptions (self) :
        return self._exceptions


    def get_all_messages (self) :
        return self._messages


    def __str__ (self) :
        return self._message


    message    = property (get_message)         # string
    object     = property (get_object)          # object type
    exceptions = property (get_all_exceptions)  # list [Exception]
    messages   = property (get_all_messages)    # list [string]



class NotImplemented(SagaException):
    """ The NotImplemented exception is raised when...
    """
    _rank = 11

    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class IncorrectURL(SagaException): 
    """ The IncorrectURL exception is raised when...
    """
    _rank = 10
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class BadParameter(SagaException): 
    """ The BadParameter exception is raised when...
    """
    _rank = 9
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class AlreadyExists(SagaException): 
    """ The AlreadyExists exception is raised when...
    """
    _rank = 8
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class DoesNotExist(SagaException):
    """ The DoesNotExist exception is raised when...
    """
    _rank = 7
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class IncorrectState(SagaException): 
    """ The IncorrectState exception is raised when...
    """
    _rank = 6
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class PermissionDenied(SagaException): 
    """ The PermissionDenied exception is raised when...
    """
    _rank = 5
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class AuthorizationFailed(SagaException): 
    """ The AuthorizationFailed exception is raised when...
    """
    _rank = 4
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class AuthenticationFailed(SagaException): 
    """ The AuthenticationFailed exception is raised when...
    """
    _rank = 3
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class Timeout(SagaException): 
    """ The Timeout exception is raised when...
    """
    _rank = 2
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


class NoSuccess(SagaException): 
    """ The NoSuccess exception is raised when...  
    """
    _rank = 1
    
    def __init__  (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

