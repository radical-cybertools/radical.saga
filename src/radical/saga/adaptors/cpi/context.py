
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from .base       import CPIBase
from .decorators import CPI_SYNC_CALL  as SYNC
from .decorators import CPI_ASYNC_CALL as ASYNC


class Context (CPIBase) :
    
    @SYNC
    def init_instance         (self, type)    : pass

    @SYNC
    def _initialize           (self, session) : pass

    @SYNC
    def _get_default_contexts (self, session) : pass




