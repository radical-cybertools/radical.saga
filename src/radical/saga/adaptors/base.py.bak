
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" the adaptor base class. """

import radical.utils         as ru
import radical.utils.logger  as rul

from   ..exceptions import *


# ------------------------------------------------------------------------------
# adaptor base class
#
class Base(object):

    # We only need one instance of this adaptor per process (actually per
    # engine, but engine is a singleton, too...) -- the engine will though
    # create new CPI implementation instances as needed (one per SAGA API
    # object).
    __metaclass__ = ru.Singleton


    # --------------------------------------------------------------------------
    #
    # FIXME: phase out adaptor_options
    #
    def __init__ (self, adaptor_info, adaptor_options=None, expand_env=True):

        # FIXME: engine is loading cfg already, here we load again...

        self._info    = adaptor_info
        self._name    = adaptor_info['name']
        self._schemas = adaptor_info['schemas']

        self._lock    = ru.RLock(self._name)
        self._logger  = ru.Logger('radical.saga.api')


        # we need to expand later once we got env from the remote resource
        self._cfg     = ru.Config(module='radical.saga', name=self._name,
                                  expand=expand_env)

        if 'enabled' not in self._cfg:
            self._cfg['enabled'] = True


    # --------------------------------------------------------------------------
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


    # --------------------------------------------------------------------------
    #
    def register (self) :
        """ Adaptor registration function. The engine calls this during startup
            to retrieve the adaptor information.
        """

        return self._info


    # --------------------------------------------------------------------------
    #
    def get_name (self) :

        return self._name


    # --------------------------------------------------------------------------
    #
    def get_schemas (self) :

        return self._schemas


    # --------------------------------------------------------------------------
    #
    def get_info (self) :

        return self._info


# ------------------------------------------------------------------------------

