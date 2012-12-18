
import saga.cpi.base
import saga.cpi.context

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

######################################################################
#
# adaptor meta data
#
_adaptor_name     =    'saga.adaptor.saga_adaptor_context_userpass'
_adaptor_registry = [{ 'name'    : _adaptor_name,
                       'type'    : 'saga.Context',
                       'class'   : 'context_userpass',
                       'schemas' : ['UserPass']
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
class context_userpass (saga.cpi.Context) :

    def __init__ (self) :
        saga.cpi.Base.__init__ (self, _adaptor_name)
        # print "userpass context adaptor init"


    @SYNC
    def init_instance (self, type) :
        # print "userpass context adaptor instance init sync %s" % id
        self._type = type


    @SYNC
    def set_default (self) :
        # print "sync set_defaults"
        pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

