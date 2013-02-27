__author__    = ["Andre Merzky", "Ole Weidner"]
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

""" Provides the CPI base class, adaptor base class, and a number of call
    decorators. """

import re
import inspect
import weakref

import saga.utils.singleton as saga_singleton
import saga.utils.logger    as saga_logger
import saga.utils.config    as saga_config

from   saga.exceptions import *
from   saga.task       import Task, SYNC, ASYNC, TASK


# ------------------------------------
# adaptor base class
#
class AdaptorBase (saga_config.Configurable) :

    

    # We only need one instance of this adaptor per process (actually per
    # engine, but engine is a singleton, too...) -- the engine will though
    # create new CPI implementation instances as needed (one per SAGA API
    # object).
    __metaclass__ = saga_singleton.Singleton


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
                'documentation'    : "Enable / disable loading of the adaptor",
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

        self._session   = None
        self._adaptor   = adaptor
        self._cpi_cname = self.__class__.__name__
        self._logger    = saga_logger.getLogger (self._cpi_cname)

        # The API object must obviously keep an adaptor instance.  If we also
        # keep an API instance ref in this adaptor base, we create a ref cycle
        # which will annoy (i.e. disable) garbage collection.  We thus use weak
        # references to break that cycle.  The inheriting classes MUST use
        # get_api() to obtain the API reference.
        if api :
            self._api   = weakref.ref (api)
        else :
            self._api   = None

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


    def get_api (self) :
        if self._api :
            # get api from weakref.  We can be quite confident that the api
            # object has *not* been garbage collected, yet, as it obviously is
            # still binding this adaptor instance.
            return self._api ()
        else :
            # no need to de-weakref 'None'
            return self._api


    def get_adaptor_name (self) :
        return self._adaptor.get_name ()


    def _set_session (self, session) :
        self._session = session


    def get_session (self) :
        return self._session


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

