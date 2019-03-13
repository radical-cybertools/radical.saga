
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from ...           import exceptions as rse

from .decorators   import CPI_SYNC_CALL  as SYNC


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
        raise rse.NotImplemented ("Callbacks not supported for this backend")





