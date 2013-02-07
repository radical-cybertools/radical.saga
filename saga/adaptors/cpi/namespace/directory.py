
import saga.adaptors.cpi.decorators as CPI
import entry

# keep order of inheritance!  super() below uses MRO
class Directory (entry.Entry) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsentry = super  (Directory, self)
        self._cpi_nsentry.__init__ (api, adaptor)

    @CPI.SYNC
    def init_instance           (self, url, flags, session)    : pass
    @CPI.ASYNC
    def init_instance_async     (self, url, flags, session)    : pass 


    # ----------------------------------------------------------------
    #
    # namespace directory methods
    #
    @CPI.SYNC
    def change_dir              (self, url, ttype)             : pass
    @CPI.ASYNC
    def change_dir_async        (self, url, ttype)             : pass

    @CPI.SYNC
    def list                    (self, npat, ttype)            : pass
    @CPI.ASYNC
    def list_async              (self, npat, ttype)            : pass

    @CPI.SYNC
    def find                    (self, npat, flags, ttype)     : pass
    @CPI.ASYNC
    def find_async              (self, npat, flags, ttype)     : pass

    @CPI.SYNC
    def exists                  (self, name, ttype)            : pass
    @CPI.ASYNC
    def exists_async            (self, name, ttype)            : pass


    @CPI.SYNC
    def is_dir                  (self, name, ttype)            : pass
    @CPI.ASYNC
    def is_dir_async            (self, name, ttype)            : pass

    @CPI.SYNC
    def is_entry                (self, name, ttype)            : pass
    @CPI.ASYNC
    def is_entry_async          (self, name, ttype)            : pass

    @CPI.SYNC
    def is_link                 (self, name, ttype)            : pass
    @CPI.ASYNC
    def is_link_async           (self, name, ttype)            : pass

    @CPI.SYNC
    def read_link               (self, name, ttype)            : pass
    @CPI.ASYNC
    def read_link_async         (self, name, ttype)            : pass

    @CPI.SYNC
    def get_num_entries         (self, ttype)                  : pass
    @CPI.ASYNC
    def get_num_entries_async   (self, ttype)                  : pass

    @CPI.SYNC
    def get_entry               (self, num, ttype)             : pass
    @CPI.ASYNC
    def get_entry_async         (self, num, ttype)             : pass

    @CPI.SYNC
    def copy                    (self, src, tgt, flags, ttype) : pass
    @CPI.ASYNC
    def copy_async              (self, src, tgt, flags, ttype) : pass

    @CPI.SYNC
    def link                    (self, src, tgt, flags, ttype) : pass
    @CPI.ASYNC
    def link_async              (self, src, tgt, flags, ttype) : pass

    @CPI.SYNC
    def move                    (self, src, tgt, flags, ttype) : pass
    @CPI.ASYNC
    def move_async              (self, src, tgt, flags, ttype) : pass

    @CPI.SYNC
    def remove                  (self, tgt, flags, ttype)      : pass
    @CPI.ASYNC
    def remove_async            (self, tgt, flags, ttype)      : pass

    @CPI.SYNC
    def make_dir                (self, tgt, flags, ttype)      : pass
    @CPI.ASYNC
    def make_dir_async          (self, tgt, flags, ttype)      : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

