
import saga.cpi.base
import saga.cpi.context

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

######################################################################
#
# adaptor meta data
#
_adaptor_schema   =    'UserPass'
_adaptor_name     =    'saga.adaptor.userpass'
_adaptor_registry = [{ 'name'    : _adaptor_name,
                       'type'    : 'saga.Context',
                       'class'   : 'ContextUserPass',
                       'schemas' : [_adaptor_schema]
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
class ContextUserPass (saga.cpi.Context) :

    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, _adaptor_name)


    @SYNC
    def init_instance (self, type) :
        
        if type.lower () != _adaptor_schema.lower () :
            raise saga.exceptions.BadParameter \
                    ("the UserPass context adaptor only handles UserPass contexts - duh!")

        self._type = type


    @SYNC
    def _initialize (self, session) :
        pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

