
from saga.cpi.base import Base, CPI_SYNC_CALL, CPI_ASYNC_CALL

class Context (Base) :
    
    @CPI_SYNC_CALL
    def __init__  (self, api) :
        pass

    @CPI_SYNC_CALL
    def init_instance (self, type) :
        pass

    @CPI_SYNC_CALL
    def _initialize (self, session) :
        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

