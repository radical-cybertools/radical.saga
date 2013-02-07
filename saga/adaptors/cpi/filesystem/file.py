
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.namespace  as ns_cpi


# keep order of inheritance!  super() below uses MRO
class File (ns_cpi.entry.Entry) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsentry = super  (File, self)
        self._cpi_nsentry.__init__ (api, adaptor)


    @CPI.SYNC
    def init_instance        (self, url, flags, session)      : pass
    @CPI.ASYNC
    def init_instance_async  (self, url, flags, session)      : pass


    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    @CPI.SYNC
    def is_file_self         (self, ttype)                    : pass
    @CPI.ASYNC
    def is_file_self_async   (self, ttype)                    : pass

    @CPI.SYNC
    def get_size_self        (self, ttype)                    : pass
    @CPI.ASYNC
    def get_size_self_async  (self, ttype)                    : pass

    @CPI.SYNC
    def read                 (self, size, ttype)              : pass
    @CPI.ASYNC
    def read_async           (self, size, ttype)              : pass

    @CPI.SYNC
    def write                (self, data, ttype)              : pass
    @CPI.ASYNC
    def write_async          (self, data, ttype)              : pass

    @CPI.SYNC
    def seek                 (self, off, whence, ttype)       : pass
    @CPI.ASYNC
    def seek_async           (self, off, whence, ttype)       : pass

    @CPI.SYNC
    def read_v               (self, iovecs, ttype)            : pass
    @CPI.ASYNC
    def read_v_async         (self, iovecs, ttype)            : pass

    @CPI.SYNC
    def write_v              (self, data, ttype)              : pass
    @CPI.ASYNC
    def write_v_async        (self, data, ttype)              : pass

    @CPI.SYNC
    def size_p               (self, pattern, ttype)           : pass
    @CPI.ASYNC
    def size_p_async         (self, pattern, ttype)           : pass

    @CPI.SYNC
    def read_p               (self, pattern, ttype)           : pass
    @CPI.ASYNC
    def read_p_async         (self, pattern, ttype)           : pass

    @CPI.SYNC
    def write_p              (self, pattern, data, ttype)     : pass
    @CPI.ASYNC
    def write_p_async        (self, pattern, data, ttype)     : pass

    @CPI.SYNC
    def modes_e              (self, ttype)                    : pass
    @CPI.ASYNC
    def modes_e_async        (self, ttype)                    : pass

    @CPI.SYNC
    def size_e               (self, emode, spec, ttype)       : pass
    @CPI.ASYNC
    def size_e_async         (self, emode, spec, ttype)       : pass

    @CPI.SYNC
    def read_e               (self, emode, spec, ttype)       : pass
    @CPI.ASYNC
    def read_e_async         (self, emode, spec, ttype)       : pass

    @CPI.SYNC
    def write_e              (self, emode, spec, data, ttype) : pass
    @CPI.ASYNC
    def write_e_async        (self, emode, spec, data, ttype) : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

