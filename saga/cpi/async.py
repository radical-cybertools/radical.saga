
from   saga.cpi.base   import Base
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async


class Async (object) :
    
    @sync
    def task_run    (self, task)          : pass

    @sync
    def task_wait   (self, task, timeout) : pass

    @sync
    def task_cancel (self, task)          : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

