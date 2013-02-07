
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.namespace  as ns_cpi
import saga.adaptors.cpi.attributes as ns_att


# keep order of inheritance!  super() below uses MRO
class Directory (ns_cpi.directory.Directory,
                 ns_att.attrbutes.Attributes) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsdirec = super  (Directory, self)
        self._cpi_nsdirec.__init__ (api, adaptor)

    @CPI.SYNC
    def init_instance           (self, url, flags, session)          : pass
    @CPI.ASYNC
    def init_instance_async     (self, url, flags, session)          : pass


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
    def set_ttl                 (self, tgt, ttl, ttype)              : pass
    @CPI.ASYNC
    def set_ttl_async           (self, tgt, ttl, ttype)              : pass

    @CPI.SYNC
    def get_ttl                 (self, tgt, ttype)                   : pass
    @CPI.ASYNC
    def get_ttl_async           (self, tgt, ttype)                   : pass

    @CPI.SYNC
    def find_adverts            (self, name_pattern, attr_pattern,
                                 obj_type, flags, ttype)             : pass
    @CPI.ASYNC
    def find_adverts_async      (self, name_pattern, attr_pattern,
                                 obj_type, flags, ttype)             : pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

