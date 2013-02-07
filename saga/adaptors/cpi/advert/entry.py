
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.namespace  as ns_cpi
import saga.adaptors.cpi.attributes as ns_att


# keep order of inheritance!  super() below uses MRO
class Entry (ns_cpi.entry.Entry, 
             ns_att.attrbutes.Attributes) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsentry = super  (Entry, self)
        self._cpi_nsentry.__init__ (api, adaptor)


    @CPI.SYNC
    def init_instance         (self, url, flags, session)      : pass
    @CPI.ASYNC
    def init_instance_async   (self, url, flags, session)      : pass


    # ----------------------------------------------------------------
    #
    # advert methods
    #
    @CPI.SYNC
    def set_ttl_self            (self, ttl, ttype=None)              : pass
    @CPI.ASYNC
    def set_ttl_self_async      (self, ttl, ttype=None)              : pass

    @CPI.SYNC
    def get_ttl_self            (self, ttype)                        : pass
    @CPI.ASYNC
    def get_ttl_self_async      (self, ttype)                        : pass

    @CPI.SYNC
    def store_object            (self, object, ttype)                : pass
    @CPI.ASYNC
    def store_object_async      (self, object, ttype)                : pass

    @CPI.SYNC
    def retrieve_object         (self, ttype)                        : pass
    @CPI.ASYNC
    def retrieve_object_async   (self, ttype)                        : pass

    @CPI.SYNC
    def delete_object           (self, ttype)                        : pass
    @CPI.ASYNC
    def delete_object_async     (self, ttype)                        : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

