
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.base       as ns_base
import saga.adaptors.cpi.async      as ns_async


# keep order of inheritance!  super() below uses MRO
class Entry (ns_base.CPIBase, ns_async.Async) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (Entry, self)
        self._cpi_base.__init__ (api, adaptor)

    @CPI.SYNC
    def init_instance        (self, url, flags, session)      : pass
    @CPI.ASYNC
    def init_instance_async  (self, url, flags, session)      : pass


    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    @CPI.SYNC
    def close                (self, timeout, ttype)           : pass
    @CPI.ASYNC
    def close_async          (self, timeout, ttype)           : pass

    @CPI.SYNC
    def get_url              (self, ttype)                    : pass
    @CPI.ASYNC
    def get_url_async        (self, ttype)                    : pass

    @CPI.SYNC
    def get_cwd              (self, ttype)                    : pass
    @CPI.ASYNC
    def get_cwd_async        (self, ttype)                    : pass

    @CPI.SYNC
    def get_name             (self, ttype)                    : pass
    @CPI.ASYNC
    def get_name_async       (self, ttype)                    : pass


    @CPI.SYNC
    def is_dir_self          (self, ttype)                    : pass
    @CPI.ASYNC
    def is_dir_self_async    (self, ttype)                    : pass

    @CPI.SYNC
    def is_entry_self        (self, ttype)                    : pass
    @CPI.ASYNC
    def is_entry_self_async  (self, ttype)                    : pass

    @CPI.SYNC
    def is_link_self         (self, ttype)                    : pass
    @CPI.ASYNC
    def is_link_self_async   (self, ttype)                    : pass

    @CPI.SYNC
    def read_link_self       (self, ttype)                    : pass
    @CPI.ASYNC
    def read_link_self_async (self, ttype)                    : pass

    @CPI.SYNC
    def copy_self            (self, tgt, flags, ttype)        : pass
    @CPI.ASYNC
    def copy_self_async      (self, tgt, flags, ttype)        : pass

    @CPI.SYNC
    def link_self            (self, tgt, flags, ttype)        : pass
    @CPI.ASYNC
    def link_self_async      (self, tgt, flags, ttype)        : pass

    @CPI.SYNC
    def move_self            (self, tgt, flags, ttype)        : pass
    @CPI.ASYNC
    def move_self_async      (self, tgt, flags, ttype)        : pass

    @CPI.SYNC
    def remove_self          (self, flags, ttype)             : pass
    @CPI.ASYNC
    def remove_self_async    (self, flags, ttype)             : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

