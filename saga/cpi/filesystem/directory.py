
from   saga.cpi.base   import Base, CPI_SYNC_CALL, CPI_ASYNC_CALL
from   saga.cpi.async  import Async

class Directory (Base, Async) :

    @CPI_SYNC_CALL
    def __init__ (self, url, flags, session) : 
        pass


    # ----------------------------------------------------------------
    #
    # filesystem directory methods
    #
    @CPI_SYNC_CALL
    def get_size (self, name, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_file (self, name, ttype) :
        pass


    @CPI_SYNC_CALL
    def open_dir (self, name, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def open (self, name, flags, ttype) :
        # if we end up here, then the callee did not manage to invoke the sync
        # adaptor call -- not much to do than raising an exception.  The
        # decorator above will take care of that
        pass

    @CPI_ASYNC_CALL
    def open_async (self, name, flags, ttype) :
        # if we end up here, then the callee did not manage to invoke the async
        # adaptor call -- the decorator above will attempt to wrap the sync call
        # in a threaded task, and use that as async op fallback
        pass
        

    # ----------------------------------------------------------------
    #
    # namespace directory methods
    #
    @CPI_SYNC_CALL
    def change_dir (self, url, ttype) :
        pass


    @CPI_SYNC_CALL
    def list (self, npat, ttype) :
        pass


    @CPI_SYNC_CALL
    def find (self, npat, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def exists (self, name, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_dir (self, name, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_entry (self, name, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_link (self, name, ttype) :
        pass


    @CPI_SYNC_CALL
    def read_link (self, name, ttype) :
        pass


    @CPI_SYNC_CALL
    def get_num_entries (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def get_entry (self, num, ttype) :
        pass


    @CPI_SYNC_CALL
    def copy (self, src, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def link (self, src, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def move (self, src, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def remove (self, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def make_dir (self, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def permissions_allow (self, tgt, id, perms, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def permissions_deny (self, tgt, id, perms, flags, ttype) :
        pass


    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    @CPI_SYNC_CALL
    def get_url (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def get_cwd (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def get_name (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_dir_self (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_entry_self (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def is_link_self (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def read_link_self (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def copy_self (self, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def link_self (self, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def move_self (self, tgt, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def remove_self (self, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def close (self, timeout, ttype) :
        pass


    @CPI_SYNC_CALL
    def permissions_allow_self (self, id, perms, flags, ttype) :
        pass


    @CPI_SYNC_CALL
    def permissions_deny_self (self, id, perms, flags, ttype) :
        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

