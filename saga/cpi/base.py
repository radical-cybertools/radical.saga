
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the CPI base class, adaptor base class, and a number of call
    decorators. """

import re
import inspect

import saga.engine.logger as saga_logger
import saga.engine.config as saga_config

from   saga.exceptions import *
from   saga.task       import Task, SYNC, ASYNC, TASK


# ------------------------------------
# adaptor base class
#
class AdaptorBase (saga_config.Configurable) :

    def __init__ (self, adaptor_info, adaptor_options=[]) :

        self._info    = adaptor_info
        self._opts    = adaptor_options
        self._name    = adaptor_info['name']
        self._schemas = adaptor_info['schemas']

        self._logger = saga_logger.getLogger (self._name)

        has_enabled = False
        for option in self._opts :
            if option['name'] == 'enabled' :
                has_enabled = True

        if not has_enabled :
            # *every* adaptor needs an 'enabled' option!
            self._opts.append ({ 
                'category'         : self._name,
                'name'             : 'enabled', 
                'type'             : bool, 
                'default'          : True, 
                'valid_options'    : [True, False],
                'documentation'    : "enable / disable %s adaptor"  % self._name,
                'env_variable'     : None
                }
            )


        saga_config.Configurable.__init__ (self, self._name, self._opts)


    # ----------------------------------------------------------------
    #
    #
    # if sanity_check() is commented out here, then we will only load adaptors
    # which implement the method themselves.
    #
    def sanity_check (self) :
        """ This method can be overloaded by adaptors to check runtime
            conditions on adaptor load time.  The adaptor should raise an
            exception if it will not be able to function properly in the given
            environment, e.g. due to missing dependencies etc.
        """
        raise BadParameter ("Adaptor %s does not implement sanity_check()"  \
                         % self._name)


    def register (self) :
        """ Adaptor registration function. The engine calls this during startup
            to retrieve the adaptor information.
        """

        return self._info


    def get_name (self) :
        return self._name


    def get_schemas (self) :
        return self._schemas


# ------------------------------------
# CPI base class
#
class CPIBase (saga_config.Configurable) :

    def __init__ (self, api, adaptor) :

        self._api       = api
        self._adaptor   = adaptor
        self._cpi_cname = self.__class__.__name__
        self._logger    = saga_logger.getLogger (self._cpi_cname)

        # by default, we assume that no bulk optimizations are supported by the
        # adaptor class.  Any adaptor class supporting bulks ops must overwrite
        # the ``_container`` attribute (via
        # ``self._set_container(container=None)``, and have it point to the
        # class which implements the respective ``container_*`` methods.
        self._container = None


    def _set_container (self, container=None) :
        self._container = container


    def get_cpi_cname (self) :
        return self._cpi_cname


    def get_adaptor_name (self) :
        return self._adaptor.get_name ()




# ------------------------------------
# decorator, which switches method to 
# _async version if ttype is set and !None
def SYNC_CALL (sync_function) :
    
    def wrap_function (self, *args, **kwargs) :

        if 'ttype' in kwargs and kwargs['ttype'] != None :

            if not kwargs['ttype'] in (SYNC, ASYNC, TASK) :
                # cannot handle that ttype value, do not call async methods
                msg = " %s: async %s() called with invalid tasktype (%s)" \
                    % (self.__class__.__name__, sync_function.__name__, str(ttype))
                raise BadParameter (msg)

            # call async method flavor
            try :
                async_function_name = "%s_async"  %  sync_function.__name__
                async_function      = getattr (self, async_function_name)

            except AttributeError :
                msg = " %s: async %s() not implemented" \
                    % (self.__class__.__name__, sync_function.__name__)
                raise NotImplemented (msg)

            else :
                # 'self' not needed, getattr() returns member function
                return async_function (*args, **kwargs)
        
        # no ttype, or ttype==None -- make sure it's gone, and call default sync
        # function
        if 'ttype' in kwargs : 
            del kwargs['ttype']

        return sync_function (self, *args, **kwargs)

    return wrap_function


# ------------------------------------
# we assume that async calls want to call, aehm, async methods...
def ASYNC_CALL (async_function) :
    return async_function



# ------------------------------------
# sync cpi calls ae only called when an adaptor does not implement that call --
# we thus raise a NotImplemented exception.
def CPI_SYNC_CALL (cpi_sync_function) :

    def wrap_function (self, *args, **kwargs) :
        raise NotImplemented ("%s.%s is not implemented for %s.%s" \
                %  (self._api.__class__.__name__, 
                    inspect.stack ()[1][3],
                    self._adaptor._name, 
                    self.__class__.__name__))

    return wrap_function

# ------------------------------------
# async cpi calls attempt to wrap sync adaptor calls into threaded tasks
def CPI_ASYNC_CALL (cpi_async_function) :

    def wrap_function (self, *args, **kwargs) :

        print cpi_async_function

        my_ttype = None
        my_call  = None
        my_args  = ()


        if not 'ttype' in kwargs :
            msg = " %s: async %s() called with no tasktype" \
                % (self.__class__.__name__, cpi_async_function.__name__)
            raise BadParameter (msg)


        ttype = kwargs['ttype']
        del kwargs['ttype']


        if not ttype in (SYNC, ASYNC, TASK) :
            # cannot handle that ttype value, do not call async methods
            msg = " %s: async %s() called with invalid tasktype (%s)" \
                % (self.__class__.__name__, cpi_async_function.__name__, str(ttype))
            raise BadParameter (msg)


        cpi_sync_function_name = None
        cpi_sync_function      = None

        # find sync method flavor
        try :
            cpi_sync_function_name = re.sub ("_async$", "", cpi_async_function.__name__)
            cpi_sync_function      = getattr (self, cpi_sync_function_name)

        except AttributeError :
            msg = " %s: sync %s() not implemented" \
                % (self.__class__.__name__, cpi_sync_function.__name__)
            raise NotImplemented (msg)


        # got the sync call, wrap it in a task

        c = { '_call'   : cpi_sync_function,
              '_args'   : args, 
              '_kwargs' : kwargs }   # no ttype!

        return Task (self, cpi_sync_function_name, c, ttype)

    return wrap_function


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

