
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from ...exceptions import *

from .decorators   import CPI_SYNC_CALL  as SYNC
from .decorators   import CPI_ASYNC_CALL as ASYNC


class Attributes (object) :
    
    @SYNC
    def attribute_getter    (self, key)         : pass

    @SYNC
    def attribute_setter    (self, key, val)    : pass

    @SYNC
    def attribute_lister    (self)              : pass

    @SYNC
    def attribute_caller    (self, key, id, cb) : pass

    @SYNC
    def add_callback        (self, key, cb)     :
        raise NotImplemented ("Callbacks are not supported for this backend")





