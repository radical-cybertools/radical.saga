
import saga.adaptors.cpi.decorators as cpi_dec

SYNC  = cpi_dec.CPI_SYNC_CALL
ASYNC = cpi_dec.CPI_ASYNC_CALL


class Attributes (object) :
    
    @SYNC
    def attribute_getter    (self, key)         : pass

    @SYNC
    def attribute_setter    (self, key, val)    : pass

    @SYNC
    def attribute_lister    (self)              : pass

    @SYNC
    def attribute_caller    (self, key, id, cb) : pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

