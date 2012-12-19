# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA runtime. """

import saga.engine.logger as saga_logger
import saga.engine.config as saga_config
import saga.exceptions
import saga.task

# ------------------------------------
# CPI base class
#
class Base (saga_config.Configurable) :

    def __init__ (self, api, adaptor_name, config_options={}) :

        self._api          = api
        self._adaptor_name = adaptor_name
        self._logger       = saga_logger.getLogger (adaptor_name)

        saga_config.Configurable.__init__ (self, adaptor_name, config_options)


    def _get_name (self) :
        return self._adaptor_name


    def _get_api (self) :
        return self._api



# ------------------------------------
# decorator, which switches method to 
# _async version if ttype is set and !None
def sync (sync_function) :
    
    def wrap_function (self, *args, **kwargs) :

        if 'ttype' in kwargs and kwargs['ttype'] != None :
            if kwargs['ttype'] != saga.task.SYNC  and \
               kwargs['ttype'] != saga.task.ASYNC and \
               kwargs['ttype'] != saga.task.TASK      :
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
                # print " ----------------> async "
                return async_function (*args, **kwargs)
        
        # no ttype, or ttype==None -- make sure it's gone, and call default sync
        # function
        if 'ttype' in kwargs : 
            del kwargs['ttype']

        # print " ----------------> sync "
        return sync_function (self, *args, **kwargs)

    return wrap_function


# ------------------------------------
# we assume that async calls want to call, aehm, async methods...
def async (async_function) :
    return async_function


