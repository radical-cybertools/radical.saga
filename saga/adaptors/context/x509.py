
import os

from   saga.utils.singleton import Singleton

import saga.context
import saga.cpi.base
import saga.cpi.context

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async


######################################################################
#
# adaptor meta data
#
_ADAPTOR_NAME          = 'saga.adaptor.x509'
_ADAPTOR_SCHEMAS       = ['X509']
_ADAPTOR_OPTIONS       = [{ 
    'category'         : _ADAPTOR_NAME,
    'name'             : 'enabled', 
    'type'             : bool, 
    'default'          : True, 
    'valid_options'    : [True, False],
    'documentation'    : "enable / disable %s adaptor"  % _ADAPTOR_NAME,
    'env_variable'     : None
    }
]

# FIXME: complete attribute list
_ADAPTOR_CAPABILITES   = {
    'attributes'       : [saga.context.TYPE,
                          saga.context.USER_PROXY,
                          saga.context.LIFE_TIME]
}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilites'      : _ADAPTOR_CAPABILITES,
    'description'      : 'The X509 context adaptor.',
    'details'          : """This adaptor points to a X509 proxy, or certificate,
                            be used for backend connections.  Note that this
                            context can be created by a MyProxy context instance.""",
    'schemas'          : {'x509' : 'x509 token information.'},
}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'cpis'             : [{ 
        'type'         : 'saga.Context',
        'class'        : 'ContextX509',
        'schemas'      : _ADAPTOR_SCHEMAS
        }
    ]
}


###############################################################################
# The adaptor class

class Adaptor (saga.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.

    We only need one instance of this adaptor per process (actually per engine,
    but engine is a singleton, too...) -- the engine will though create new CPI
    implementation instances as needed (one per SAGA API object).
    """

    __metaclass__ = Singleton


    def __init__ (self) :

        saga.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)


    def sanity_check (self) :
        pass



######################################################################
#
# job adaptor class
#
class ContextX509 (saga.cpi.Context) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'ContextX509')


    @SYNC
    def init_instance (self, type) :

        if not type.lower () in (schema.lower() for schema in _ADAPTOR_SCHEMAS) :
            raise saga.exceptions.BadParameter \
                    ("the x509 context adaptor only handles x509 contexts - duh!")

        self._api.type = type


    @SYNC
    def _initialize (self, session) :

        # make sure we have can access the proxy
        api = self._api

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

