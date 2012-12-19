
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
                       'class'   : 'ContextUserPass',
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
class ContextUserPass (saga.cpi.Context) :

    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, _adaptor_name)
        # print "userpass context adaptor init"


    @SYNC
    def init_instance (self, type) :
        # print "userpass context adaptor instance init sync %s" % id
        self._type = type


    @SYNC
    def _initialize (self, session) :
        # print "sync _initialize"
        pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

