
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.namespace  as ns_cpi


# keep order of inheritance!  super() below uses MRO
class LogicalDirectory (ns_cpi.directory.Directory) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsdirec = super  (LogicalDirectory, self)
        self._cpi_nsdirec.__init__ (api, adaptor)

    @CPI.SYNC
    def init_instance           (self, url, flags, session)          : pass
    @CPI.ASYNC
    def init_instance_async     (self, url, flags, session)          : pass


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    @CPI.SYNC
    def is_file                 (self, tgt, ttype)                   : pass
    @CPI.ASYNC
    def is_file_async           (self, tgt, ttype)                   : pass

    @CPI.SYNC
    def is_file_self            (self, ttype)                        : pass
    @CPI.ASYNC
    def is_file_self_async      (self, ttype)                        : pass

    @CPI.SYNC
    def find_replicas       (self, name_pattern, attr_pattern, flags, ttype)  : pass
    @CPI.ASYNC
    def find_replicas_async (self, name_pattern, attr_pattern, flags, ttype)  : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

