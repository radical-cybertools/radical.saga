

from saga.cpi.base import CPI_SYNC_CALL, CPI_ASYNC_CALL

class Async (object) :
    
    @CPI_SYNC_CALL
    def __init__ (self, api) :
        pass

    @CPI_SYNC_CALL
    def task_run (self, task) :
        pass

    @CPI_SYNC_CALL
    def task_wait (self, task, timeout) :
        pass

    @CPI_SYNC_CALL
    def task_cancel (self, task) :
        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

