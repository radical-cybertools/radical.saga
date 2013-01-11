
from   saga.cpi.base   import CPIBase
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async


class Context (CPIBase) :
    
    @sync
    def init_instance         (self, type)    : pass

    @sync
    def _initialize           (self, session) : pass

    @sync
    def _get_default_contexts (self, session) : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

