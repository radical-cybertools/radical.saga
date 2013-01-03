
from   saga.cpi.base   import CPIBase
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async
from   saga.cpi.async  import Async


class LogicalDirectory (CPIBase, Async) :

    @sync
    def init_instance           (self, url, flags, session) : pass
    @async
    def init_instance_async     (self, url, flags, session) : pass


    # ----------------------------------------------------------------
    #
    # replica directory methods 
    #
    @sync
    def is_file                 (self, name, ttype)                  : pass
    @async
    def is_file_async           (self, name, ttype)                  : pass

    @sync
    def open_dir                (self, name, flags, ttype)           : pass
    @async
    def open_dir_async          (self, name, flags, ttype)           : pass

    @sync
    def open                    (self, name, flags, ttype)           : pass
    @async
    def open_async              (self, name, flags, ttype)           : pass


    # ----------------------------------------------------------------
    #
    # namespace directory methods
    #
    @sync
    def change_dir              (self, url, ttype)                   : pass
    @async
    def change_dir_async        (self, url, ttype)                   : pass

    @sync
    def list                    (self, npat, ttype)                  : pass
    @async
    def list_async              (self, npat, ttype)                  : pass

    @sync
    def find                    (self, npat, flags, ttype)           : pass
    @async
    def find_async              (self, npat, flags, ttype)           : pass

    @sync
    def exists                  (self, name, ttype)                  : pass
    @async
    def exists_async            (self, name, ttype)                  : pass

    @sync
    def is_dir                  (self, name, ttype)                  : pass
    @async
    def is_dir_async            (self, name, ttype)                  : pass

    @sync
    def is_entry                (self, name, ttype)                  : pass
    @async
    def is_entry_async          (self, name, ttype)                  : pass

    @sync
    def is_link                 (self, name, ttype)                  : pass
    @async
    def is_link_async           (self, name, ttype)                  : pass

    @sync
    def read_link               (self, name, ttype)                  : pass
    @async
    def read_link_async         (self, name, ttype)                  : pass

    @sync
    def get_num_entries         (self, ttype)                        : pass
    @async
    def get_num_entries_async   (self, ttype)                        : pass

    @sync
    def get_entry               (self, num, ttype)                   : pass
    @async
    def get_entry_async         (self, num, ttype)                   : pass

    @sync
    def copy                    (self, src, tgt, flags, ttype)       : pass
    @async
    def copy_async              (self, src, tgt, flags, ttype)       : pass

    @sync
    def link                    (self, src, tgt, flags, ttype)       : pass
    @async
    def link_async              (self, src, tgt, flags, ttype)       : pass

    @sync
    def move                    (self, src, tgt, flags, ttype)       : pass
    @async
    def move_async              (self, src, tgt, flags, ttype)       : pass

    @sync
    def remove                  (self, tgt, flags, ttype)            : pass
    @async
    def remove_async            (self, tgt, flags, ttype)            : pass

    @sync
    def make_dir                (self, tgt, flags, ttype)            : pass
    @async
    def make_dir_async          (self, tgt, flags, ttype)            : pass

    @sync
    def permissions_allow       (self, tgt, id, perms, flags, ttype) : pass
    @async
    def permissions_allow_async (self, tgt, id, perms, flags, ttype) : pass

    @sync
    def permissions_deny        (self, tgt, id, perms, flags, ttype) : pass
    @async
    def permissions_deny_async  (self, tgt, id, perms, flags, ttype) : pass


    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    @sync
    def get_url                 (self, ttype)                        : pass
    @async
    def get_url_async           (self, ttype)                        : pass

    @sync
    def get_cwd                 (self, ttype)                        : pass
    @async
    def get_cwd_async           (self, ttype)                        : pass

    @sync
    def get_name                (self, ttype)                        : pass
    @async
    def get_name_async          (self, ttype)                        : pass

    @sync
    def is_dir_self             (self, ttype)                        : pass
    @async
    def is_dir_self_async       (self, ttype)                        : pass

    @sync
    def is_entry_self           (self, ttype)                        : pass
    @async
    def is_entry_self_async     (self, ttype)                        : pass

    @sync
    def is_link_self            (self, ttype)                        : pass
    @async
    def is_link_self_async      (self, ttype)                        : pass

    @sync
    def read_link_self          (self, ttype)                        : pass
    @async
    def read_link_self_async    (self, ttype)                        : pass

    @sync
    def copy_self               (self, tgt, flags, ttype)            : pass
    @async
    def copy_self_async         (self, tgt, flags, ttype)            : pass

    @sync
    def link_self               (self, tgt, flags, ttype)            : pass
    @async
    def link_self_async         (self, tgt, flags, ttype)            : pass

    @sync
    def move_self               (self, tgt, flags, ttype)            : pass
    @async
    def move_self_async         (self, tgt, flags, ttype)            : pass

    @sync
    def remove_self             (self, flags, ttype)                 : pass
    @async
    def remove_self_async       (self, flags, ttype)                 : pass

    @sync
    def close                   (self, timeout, ttype)               : pass
    @async
    def close_async             (self, timeout, ttype)               : pass

    @sync
    def permissions_allow_self       (self, id, perms, flags, ttype) : pass
    @async
    def permissions_allow_self_async (self, id, perms, flags, ttype) : pass

    @sync
    def permissions_deny_self        (self, id, perms, flags, ttype) : pass
    @async
    def permissions_deny_self_async  (self, id, perms, flags, ttype) : pass


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    @sync
    def is_file                 (self, tgt, ttype)                   : pass
    @async
    def is_file_async           (self, tgt, ttype)                   : pass

    @sync
    def is_file_self            (self, ttype)                        : pass
    @async
    def is_file_self_async      (self, ttype)                        : pass

    @sync
    def open_dir                (self, name, flags, ttype)           : pass
    @async
    def open_dir_async          (self, name, flags, ttype)           : pass

    @sync
    def open                    (self, name, flags, ttype)           : pass
    @async
    def open_async              (self, name, flags, ttype)           : pass

    @sync
    def find       (self, name_pattern, attr_pattern, flags, ttype)  : pass
    @async
    def find_async (self, name_pattern, attr_pattern, flags, ttype)  : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

