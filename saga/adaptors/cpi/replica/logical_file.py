
from   saga.cpi.base   import CPIBase
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async
from   saga.cpi.async  import Async


class LogicalFile (CPIBase, Async) :

    @sync
    def __init__              (self, url, flags, session)      : pass
    @async
    def __init___async        (self, url, flags, session)      : pass


    # ----------------------------------------------------------------
    #
    # namespace entry methods 
    #
    @sync
    def get_url               (self, ttype)                    : pass
    @async
    def get_url_async         (self, ttype)                    : pass

    @sync
    def get_cwd               (self, ttype)                    : pass
    @async
    def get_cwd_async         (self, ttype)                    : pass

    @sync
    def get_name              (self, ttype)                    : pass
    @async
    def get_name_async        (self, ttype)                    : pass

    @sync
    def is_dir_self           (self, ttype)                    : pass
    @async
    def is_dir_self_async     (self, ttype)                    : pass

    @sync
    def is_entry_self         (self, ttype)                    : pass
    @async
    def is_entry_self_async   (self, ttype)                    : pass

    @sync
    def is_link_self          (self, ttype)                    : pass
    @async
    def is_link_self_async    (self, ttype)                    : pass

    @sync
    def read_link_self        (self, ttype)                    : pass
    @async
    def read_link_self_async  (self, ttype)                    : pass

    @sync
    def copy_self             (self, tgt, flags, ttype)        : pass
    @async
    def copy_self_async       (self, tgt, flags, ttype)        : pass

    @sync
    def link_self             (self, tgt, flags, ttype)        : pass
    @async
    def link_self_async       (self, tgt, flags, ttype)        : pass

    @sync
    def move_self             (self, tgt, flags, ttype)        : pass
    @async
    def move_self_async       (self, tgt, flags, ttype)        : pass

    @sync
    def remove_self           (self, flags, ttype)             : pass
    @async
    def remove_self_async     (self, flags, ttype)             : pass

    @sync
    def close                 (self, timeout, ttype)           : pass
    @async
    def close_async           (self, timeout, ttype)           : pass


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    @sync
    def is_file_self          (self, ttype)                    : pass
    @async
    def is_file_self_async    (self, ttype)                    : pass

    @sync
    def get_size_self         (self, ttype)                    : pass
    @async
    def get_size_self_async   (self, ttype)                    : pass

    @sync
    def read                  (self, size, ttype)              : pass
    @async
    def read_async            (self, size, ttype)              : pass

    @sync
    def write                 (self, data, ttype)              : pass
    @async
    def write_async           (self, data, ttype)              : pass

    @sync
    def seek                  (self, off, whence, ttype)       : pass
    @async
    def seek_async            (self, off, whence, ttype)       : pass

    @sync
    def read_v                (self, iovecs, ttype)            : pass
    @async
    def read_v_async          (self, iovecs, ttype)            : pass

    @sync
    def write_v               (self, data, ttype)              : pass
    @async
    def write_v_async         (self, data, ttype)              : pass

    @sync
    def size_p                (self, pattern, ttype)           : pass
    @async
    def size_p_async          (self, pattern, ttype)           : pass

    @sync
    def read_p                (self, pattern, ttype)           : pass
    @async
    def read_p_async          (self, pattern, ttype)           : pass

    @sync
    def write_p               (self, pattern, data, ttype)     : pass
    @async
    def write_p_async         (self, pattern, data, ttype)     : pass

    @sync
    def modes_e               (self, ttype)                    : pass
    @async
    def modes_e_async         (self, ttype)                    : pass

    @sync
    def size_e                (self, emode, spec, ttype)       : pass
    @async
    def size_e_async          (self, emode, spec, ttype)       : pass

    @sync
    def read_e                (self, emode, spec, ttype)       : pass
    @async
    def read_e_async          (self, emode, spec, ttype)       : pass

    @sync
    def write_e               (self, emode, spec, data, ttype) : pass
    @async
    def write_e_async         (self, emode, spec, data, ttype) : pass


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    @sync
    def is_file_self          (self, ttype)                    : pass
    @async
    def is_file_self_async    (self, ttype)                    : pass

    @sync
    def add_location          (self, name, ttype)              : pass
    @async
    def add_location_async    (self, name, ttype)              : pass

    @sync
    def remove_location       (self, name, ttype)              : pass
    @async
    def remove_location_async (self, name, ttype)              : pass

    @sync
    def update_location       (self, old, new, ttype)          : pass
    @async
    def update_location_async (self, old, new, ttype)          : pass

    @sync
    def list_locations        (self, ttype)                    : pass
    @async
    def list_locations_async  (self, ttype)                    : pass

    @sync
    def replicate             (self, name, flags, ttype)       : pass
    @async
    def replicate_async       (self, name, flags, ttype)       : pass

    @sync
    def upload                (self, name, tgt, flags, ttype)  : pass
    @async
    def upload_async          (self, name, tgt, flags, ttype)  : pass

    @sync
    def download              (self, name, src, flags, ttype)  : pass
    @async
    def download_async        (self, name, src, flags, ttype)  : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

