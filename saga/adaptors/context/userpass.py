
from   saga.utils.singleton import Singleton

import saga.cpi.base
import saga.cpi.context

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

######################################################################
#
# adaptor meta data
#
_adaptor_schemas  = ['UserPass']
_adaptor_name     = 'saga.adaptor.userpass'
_adaptor_options  = []
_adaptor_info     = {
    'name'        : _adaptor_name,
    'version'     : 'v0.1',
    'cpis'        : [{ 
        'type'    : 'saga.Context',
        'class'   : 'ContextUserPass',
        'schemas' : _adaptor_schemas
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

        saga.cpi.base.AdaptorBase.__init__ (self, _adaptor_name, _adaptor_options)


    def register (self) :
        """ Adaptor registration function. The engine calls this during startup. 
    
            We usually do sanity checks here and throw and exception if we think
            the adaptor won't work in a given environment. In that case, the
            engine won't add it to it's internal list of adaptors. If everything
            is ok, we return the adaptor info.
        """
    
        return _adaptor_info


######################################################################
#
# job adaptor class
#
class ContextUserPass (saga.cpi.Context) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'ContextUserPass')


    @SYNC
    def init_instance (self, type) :
        
        if not type.lower () in (schema.lower() for schema in _adaptor_schemas) :
            raise saga.exceptions.BadParameter \
                    ("the UserPass context adaptor only handles UserPass contexts - duh!")

        self._type = type


    @SYNC
    def _initialize (self, session) :
        pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

