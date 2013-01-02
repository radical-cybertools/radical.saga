
from   saga.cpi.base   import Base
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async


class Context (Base) :
    
    @sync
    def init_instance (self, type)    : pass

    @sync
    def _initialize   (self, session) : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

