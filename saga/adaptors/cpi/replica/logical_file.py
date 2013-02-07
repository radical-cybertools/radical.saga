
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.namespace  as ns_cpi


# keep order of inheritance!  super() below uses MRO
class LogicalFile (ns_cpi.entry.Entry) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsentry = super  (LogicalFile, self)
        self._cpi_nsentry.__init__ (api, adaptor)


    @CPI.SYNC
    def init_instance         (self, url, flags, session)      : pass
    @CPI.ASYNC
    def init_instance_async   (self, url, flags, session)      : pass


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    @CPI.SYNC
    def is_file_self          (self, ttype)                    : pass
    @CPI.ASYNC
    def is_file_self_async    (self, ttype)                    : pass

    @CPI.SYNC
    def add_location          (self, name, ttype)              : pass
    @CPI.ASYNC
    def add_location_async    (self, name, ttype)              : pass

    @CPI.SYNC
    def remove_location       (self, name, ttype)              : pass
    @CPI.ASYNC
    def remove_location_async (self, name, ttype)              : pass

    @CPI.SYNC
    def update_location       (self, old, new, ttype)          : pass
    @CPI.ASYNC
    def update_location_async (self, old, new, ttype)          : pass

    @CPI.SYNC
    def list_locations        (self, ttype)                    : pass
    @CPI.ASYNC
    def list_locations_async  (self, ttype)                    : pass

    @CPI.SYNC
    def replicate             (self, name, flags, ttype)       : pass
    @CPI.ASYNC
    def replicate_async       (self, name, flags, ttype)       : pass

    @CPI.SYNC
    def upload                (self, name, tgt, flags, ttype)  : pass
    @CPI.ASYNC
    def upload_async          (self, name, tgt, flags, ttype)  : pass

    @CPI.SYNC
    def download              (self, name, src, flags, ttype)  : pass
    @CPI.ASYNC
    def download_async        (self, name, src, flags, ttype)  : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

