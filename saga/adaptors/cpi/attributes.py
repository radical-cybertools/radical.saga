
from   saga.cpi.base   import CPIBase
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async


class Attributes (object) :
    
    @sync
    def attribute_getter    (self, key)         : pass

    @sync
    def attribute_setter    (self, key, val)    : pass

    @sync
    def attribute_lister    (self)              : pass

    @sync
    def attribute_caller    (self, key, id, cb) : pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

