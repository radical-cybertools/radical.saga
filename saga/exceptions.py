__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

""" Exception classes
"""

import weakref
import operator

import saga.utils.exception


class SagaException(saga.utils.exception.ExceptionBase):
    """
    The Exception class encapsulates information about error conditions
    encountered in SAGA.

    Additionally to the error message (e.message), the exception also provides
    a trace to the code location where the error condition got raised
    (e.traceback).

    B{Example}::

      try :
          file = saga.filesystem.File ("sftp://alamo.futuregrid.org/tmp/data1.dat")

      except saga.Timeout as to :
          # maybe the network is down?
          print "connection timed out"
      except saga.Exception as e :
          # something else went wrong
          print "Exception occurred: %s %s" % (e, e.traceback)

    There are cases where multiple backends can report errors at the same time.
    In that case, the saga-python implementation will collect the exceptions,
    sort them by their 'rank', and return the highest ranked one.  All other
    catched exceptions are available via :func:`get_all_exceptions`, or via the
    `exceptions` property.

    The rank of an exception defines its explicity: in general terms: the higher
    the rank, the better defined / known is the cause of the problem.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, message, api_object=None):
        """ 
        Create a new exception object.

        :param message: The exception message.
        :param object:  The object that has caused the exception, default is
                        None.
        """
        saga.utils.exception.ExceptionBase.__init__ (self, message)

        self._type          = self.__class__.__name__
        self._message       = message
        self._messages      = [self.get_message ()]
        self._exceptions    = [self]
        self._top_exception = self
        self._traceback     = saga.utils.exception.get_traceback (1)

        if api_object : 
            self._object    = weakref.ref (api_object)
        else :
            self._object    = None

    # ----------------------------------------------------------------
    #
    def _clone (self) :
        """ This method is used internally -- see :func:`_get_exception_stack`."""

        clone = self.__class__ (self._message, self._object)
        clone._messages  = self._messages
        clone._exception = self._exceptions
        clone._traceback = self._traceback
        clone._type      = self._type

        return clone


    # ----------------------------------------------------------------
    #
    def get_message (self) :
        """ Return the exception message as a string.  That message is also
        available via the 'message' property."""
        return "%s: %s" % (self.type, self._message)

    def get_type (self):
        """ Return the type of the exception as string.
        """
        return self._type


    # ----------------------------------------------------------------
    #
    def get_object (self) :
        """ Return the object that raised this exception. An object may not
        always be available -- for example, exceptions raised during object
        creation may not have the option to keep an incomplete object instance
        around.  In those cases, this method will return 'None'.  Either way,
        the object is also accessible via the 'object' property.
        """
        o = None

        if self._object :
            o = self._object ()

            if o is None:
                # object has been garbage collected - we simply won't return
                # anything then...
                pass

        return o


    # ----------------------------------------------------------------
    #
    def _add_exception (self, e) :
        """
        Some sub-operation raised a SAGA exception, but other exceptions may
        be catched later on.  In that case the later exceptions can be added to
        the original one with :func:`_add_exception`\(e).  Once all exceptions are
        collected, a call to :func:`_get_exception_stack`\() will return a new
        exception which is selected from the stack by rank and order.  All other
        exceptions can be accessed from the returned exception by
        :func:`get_all_exceptions`\() -- those exceptions are then also ordered 
        by rank.
        """

        self._exceptions.append (e)
        self._messages.append   (e.message)

        if e._rank > self._top_exception._rank :
            self._top_exception = e


    # ----------------------------------------------------------------
    #
    def _get_exception_stack (self) :
        """ 
        This method is internally used by the saga-python engine, and is only
        relevant for operations which (potentially) bind to more than one
        adaptor.
        """

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

                
    # ----------------------------------------------------------------
    #
    def get_all_exceptions (self) :
        return self._exceptions


    # ----------------------------------------------------------------
    #
    def get_all_messages (self) :
        return self._messages


    # ----------------------------------------------------------------
    #
    def __str__ (self) :
        return self.get_message ()


    message    = property (get_message)         # string
    object     = property (get_object)          # object type
    type       = property (get_type)            # exception type
    exceptions = property (get_all_exceptions)  # list [Exception]
    messages   = property (get_all_messages)    # list [string]


# --------------------------------------------------------------------
#
class NotImplemented(SagaException):
    """ SAGA-Python does not implement this method or class. (rank: 11)"""

    _rank = 11

    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class IncorrectURL(SagaException): 
    """ The given URL could not be interpreted, for example due to an incorrect
        / unknown schema. (rank: 10)"""

    _rank = 10
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class BadParameter(SagaException): 
    """ A given parameter is out of bound or ill formatted. (rank: 9)"""

    _rank = 9
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class AlreadyExists(SagaException): 
    """ The entity to be created already exists. (rank: 8)"""

    _rank = 8
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class DoesNotExist(SagaException):
    """ An operation tried to access a non-existing entity. (rank: 7)"""

    _rank = 7
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class IncorrectState(SagaException): 
    """ The operation is not allowed on the entity in its current state. (rank: 6)"""

    _rank = 6
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class PermissionDenied(SagaException): 
    """ The used identity is not permitted to perform the requested operation. (rank: 5)"""

    _rank = 5
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class AuthorizationFailed(SagaException): 
    """ The backend could not establish a valid identity. (rank: 4)"""
    _rank = 4
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class AuthenticationFailed(SagaException): 
    """ The backend could not establish a valid identity. (rank: 3)"""

    _rank = 3
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class Timeout(SagaException): 
    """ The interaction with the backend times out. (rank: 2)"""

    _rank = 2
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)


# --------------------------------------------------------------------
#
class NoSuccess(SagaException): 
    """ Some other error occurred. (rank: 1)"""

    _rank = 1
    
    def __init__ (self, msg, obj=None) :
        SagaException.__init__ (self, msg, obj)
