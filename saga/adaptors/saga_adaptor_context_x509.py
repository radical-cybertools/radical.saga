
import os

import saga.cpi.base
import saga.cpi.context

from   saga.engine.logger import getLogger

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async


######################################################################
#
# adaptor meta data
#
_adaptor_type     =    'X509'
_adaptor_name     =    'saga_adaptor_context_x509'
_adaptor_registry = [{ 'name'    : _adaptor_name,
                       'type'    : 'saga.Context',
                       'class'   : 'ContextX509',
                       'schemas' : ['X509']
                     }]


######################################################################
#
# adaptor registration
#
def register () :

    # perform some sanity checks, like check if dependencies are met
    return _adaptor_registry


######################################################################
#
# job adaptor class
#
class ContextX509 (saga.cpi.Context) :

    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, _adaptor_name)

        self._logger = getLogger (_adaptor_name)


    @SYNC
    def init_instance (self, type) :

        if type.lower () != _adaptor_type.lower () :
            raise saga.exceptions.BadParameter \
                    ("the x509 context adaptor only handles x509 contexts - duh!")

        self._api.type = type


    @SYNC
    def _initialize (self, session) :

        # make sure we have can access the proxy
        api = self._get_api ()

        if api.user_proxy :
            if not os.path.exists (api.user_proxy) or \
               not os.path.isfile (api.user_proxy)    :
                raise saga.exceptions.BadParameter ("X509 proxy does not exist: %s"
                                                 % api.user_proxy)

        try :
            fh = open (api.user_proxy)
        except Exception as e:
            raise saga.exceptions.PermissionDenied ("X509 proxy '%s' not readable: %s"
                                                 % (api.user_proxy, str(e)))
        else :
            fh.close ()

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

