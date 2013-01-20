
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides exception handling utilities and base classes.
"""

import pdb
import traceback

def get_traceback (limit=1) :
    """ Returns the current stacktrace as string.
    """

    limit += 2  # ignore local stack
    ret = ""

    stack  = traceback.extract_stack ()
    frames = traceback.format_list (stack)

    # ignore last <limit> frames
    for i in range (0, limit) :
        del frames[-1]

    # dump frames into string
    for frame in frames :
        ret += str(frame)

    return ret


def breakpoint () :
    """ set a breakpoint
    """
    pdb.pm()




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

    @classmethod
    def _log (self, logger, message, level='error'):
        """ this class method allows to log the exception message while
            constructing a SAGA exception, like::

              # raise an exception, no logging
              raise saga.IncorrectState ("File is not open")

              # raise an exception, log as error event (error level is default)
              raise saga.IncorrectState._log (self._logger, "File is not open")

              # raise an exception, log as warning event
              raise saga.IncorrectState._log (self._logger, "File is not open", level=warning)
              raise saga.IncorrectState._log (self._logger, "File is not open", warning) # same

            This way, the 'raise' remains clearly in the code, as that is the
            dominating semantics of the call.
        """

        log_method = logger.error

        try :
            log_method = getattr (logger, level.lower())
        except :
            sys.stderr.write ("unknown log level '%s'"  %  level)

        log_method (message)

        return self (message)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

