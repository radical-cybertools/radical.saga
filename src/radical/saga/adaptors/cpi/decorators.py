
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Provides a number of call decorators. """

import re
import inspect

from ...task       import Task  as T_Task   # class
from ...constants  import SYNC  as T_SYNC
from ...constants  import ASYNC as T_ASYNC
from ...constants  import TASK  as T_TASK   # flag

from ...           import exceptions as rse


# ------------------------------------------------------------------------------
# decorator, which switches method to
# _async version if ttype is set and !None
def SYNC_CALL (sync_function) :

    def wrap_function (self, *args, **kwargs) :

        if 'ttype' in kwargs and kwargs['ttype']:

            if not kwargs['ttype'] in (T_SYNC, T_ASYNC, T_TASK) :
                # cannot handle that ttype value, do not call async methods
                ttype = kwargs['ttype']
                msg   = " %s: async %s() called with invalid tasktype (%s)" \
                      % (self.__class__.__name__, sync_function.__name__,
                         str(ttype))
                raise rse.BadParameter (msg)

            # call async method flavor
            try :
                async_function_name = "%s_async"  %  sync_function.__name__
                async_function      = getattr (self, async_function_name)

            except AttributeError as e:
                msg = " %s: async %s() not implemented" \
                    % (self.__class__.__name__, sync_function.__name__)
                raise rse.NotImplemented (msg) from e

            else :
                # 'self' not needed, getattr() returns member function
                return async_function (*args, **kwargs)

        # no ttype, or ttype==None -- make sure it's gone, and call default sync
        # function
        if 'ttype' in kwargs :
            del kwargs['ttype']

        # only some functions will provide metrics, and thus need the _from_task
        # parameter -- strip that as well if its not needed
        if '_from_task' in kwargs:
            if '_from_task' not in inspect.getargspec (sync_function).args:
                del(kwargs['_from_task'])

        return sync_function (self, *args, **kwargs)

    return wrap_function


# ------------------------------------------------------------------------------
# we assume that async calls want to call, aehm, async methods...
def ASYNC_CALL (async_function) :
    return async_function


# ------------------------------------------------------------------------------
# sync cpi calls are only called when an adaptor does not implement that call --
# we thus raise a NotImplemented exception.
#
def CPI_SYNC_CALL (cpi_sync_function) :

    def wrap_function (self, *args, **kwargs) :
        raise rse.NotImplemented ("%s.%s is not implemented for %s.%s (%s)"
                %  (self.get_api ().__class__.__name__,
                    inspect.stack ()[1][3],
                    self._adaptor._name,
                    self.__class__.__name__,
                    inspect.getmembers (cpi_sync_function)[15][1]))

    return wrap_function


# ------------------------------------
# async cpi calls attempt to wrap sync adaptor calls into threaded tasks
def CPI_ASYNC_CALL (cpi_async_function) :

    def wrap_function (self, *args, **kwargs) :


        if 'ttype' not in kwargs :
            msg = " %s: async %s() called with no tasktype" \
                % (self.__class__.__name__, cpi_async_function.__name__)
            raise rse.BadParameter (msg)


        ttype = kwargs['ttype']
        del kwargs['ttype']


        if ttype not in (T_SYNC, T_ASYNC, T_TASK) :
            # cannot handle that ttype value, do not call async methods
            msg = " %s: async %s() called with invalid tasktype (%s)" \
                % (self.__class__.__name__, cpi_async_function.__name__,
                   str(ttype))
            raise rse.BadParameter (msg)


        cpi_sync_function_name = None
        cpi_sync_function      = None

        # find sync method flavor
        try :
            cpi_sync_function_name = re.sub ("_async$", "",
                                             cpi_async_function.__name__)
            cpi_sync_function      = getattr (self, cpi_sync_function_name)

        except AttributeError as e:
            msg = " %s: sync %s() not implemented" \
                % (self.__class__.__name__, cpi_sync_function.__name__)
            raise rse.NotImplemented (msg) from e


        # got the sync call, wrap it in a task
        c = {'_call'   : cpi_sync_function,
             '_args'   : args,
             '_kwargs' : kwargs }   # no ttype!

        return T_Task(self, cpi_sync_function_name, c, ttype)

    return wrap_function


# ------------------------------------------------------------------------------

