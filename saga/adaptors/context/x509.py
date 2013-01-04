
import os

from   saga.utils.singleton import Singleton

import saga.context
import saga.cpi.base
import saga.cpi.context

SYNC_CALL  = saga.cpi.base.SYNC_CALL
ASYNC_CALL = saga.cpi.base.ASYNC_CALL

######################################################################
#
# adaptor meta data
#
_ADAPTOR_NAME          = 'saga.adaptor.x509'
_ADAPTOR_SCHEMAS       = ['X509']
_ADAPTOR_OPTIONS       = []

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
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{ 
        'type'         : 'saga.Context',
        'class'        : 'ContextX509'
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

        # there are no default myproxy contexts
        self._default_contexts = []
        self._have_defaults    = False


    def sanity_check (self) :
        pass


    def _get_default_contexts (self) :

        if not self._have_defaults :

            p = "/tmp/x509up_u%d"  %  os.getuid()
            print p

            if  os.path.exists (p) and \
                os.path.isfile (p)     :

                try :
                    fh = open (p)

                except Exception as e:
                    pass

                else :
                    fh.close ()

                    c = saga.Context ('X509')
                    c.user_proxy = p

                    self._logger.info ("default X509 context for proxy at %s"  %  p)

                    self._default_contexts.append (c)
                    self._have_defaults = True

        # have defaults, and can return them...
        return self._default_contexts



######################################################################
#
# job adaptor class
#
class ContextX509 (saga.cpi.Context) :

    def __init__ (self, api, adaptor) :

        saga.cpi.CPIBase.__init__ (self, api, adaptor)


    @SYNC_CALL
    def init_instance (self, adaptor_state, type) :

        if not type.lower () in (schema.lower() for schema in _ADAPTOR_SCHEMAS) :
            raise saga.exceptions.BadParameter \
                    ("the x509 context adaptor only handles x509 contexts - duh!")

        self._api.type = type

        return self


    @SYNC_CALL
    def _initialize (self, session) :

        # make sure we have can access the proxy
        api = self._api

        if not self._api.user_proxy :
          self._api.user_proxy = "x509up_u%d"  %  os.getuid()

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

