# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA runtime. """

import saga.engine.logger as saga_logger
import saga.engine.config as saga_config

import saga.exceptions

from   saga.task import SYNC, ASYNC, TASK

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
        raise saga.exceptions.BadParameter ("Adaptor %s does not implement sanity_check()"  \
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
class Base (saga_config.Configurable) :

    def __init__ (self, api, adaptor, cpi_cname) :

        self._api       = api
        self._adaptor   = adaptor
        self._cpi_cname = cpi_cname
        self._logger    = saga_logger.getLogger (cpi_cname)

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
def sync (sync_function) :
    
    def wrap_function (self, *args, **kwargs) :

        if 'ttype' in kwargs and kwargs['ttype'] != None :
            if kwargs['ttype'] != SYNC  and \
               kwargs['ttype'] != ASYNC and \
               kwargs['ttype'] != TASK      :
                # cannot handle that ttype value, do not call async methods
                msg = " %s: async %s() called with invalid tasktype (%s)" \
                    % (self.__class__.__name__, sync_function.__name__, str(ttype))
                raise saga.exceptions.BadParameter (msg)

            # call async method flavor
            try :
                async_function_name = "%s_async"  %  sync_function.__name__
                async_function      = getattr (self, async_function_name)

            except AttributeError :
                msg = " %s: async %s() not implemented" \
                    % (self.__class__.__name__, sync_function.__name__)
                raise saga.exceptions.NotImplemented (msg)

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
def async (async_function) :
    return async_function


