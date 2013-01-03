
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
_ADAPTOR_NAME          = 'saga.adaptor.userpass'
_ADAPTOR_SCHEMAS       = ['UserPass']
_ADAPTOR_OPTIONS       = []

_ADAPTOR_CAPABILITES   = {
    'attributes'       : [saga.context.TYPE,
                          saga.context.USER_ID,
                          saga.context.USER_PASS]
}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilites'      : _ADAPTOR_CAPABILITES,
    'description'      : 'The UserPass context adaptor.',
    'details'          : """This adaptor stores user_id and user_pass tokens, to
                            be used for backend connections.""",
    'schemas'          : {'userpass' : 'this adaptor can only store username/password pairs.'},
}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{ 
        'type'         : 'saga.Context',
        'class'        : 'ContextUserPass'
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
class ContextUserPass (saga.cpi.Context) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'ContextUserPass')


    @SYNC_CALL
    def init_instance (self, adaptor_state, type) :
        
        if not type.lower () in (schema.lower() for schema in _ADAPTOR_SCHEMAS) :
            raise saga.exceptions.BadParameter \
                    ("the UserPass context adaptor only handles UserPass contexts - duh!")

        self._type = type

        return self


    @SYNC_CALL
    def _initialize (self, session) :
        pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

