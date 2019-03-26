
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from .decorators import CPI_SYNC_CALL  as SYNC
from .decorators import CPI_ASYNC_CALL as ASYNC


class Async (object) :
    
    @SYNC
    def task_run    (self, task)          : pass

    @SYNC
    def task_wait   (self, task, timeout) : pass

    @SYNC
    def task_cancel (self, task)          : pass




