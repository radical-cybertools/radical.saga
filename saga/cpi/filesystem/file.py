
from   saga.cpi.base   import Base, CPI_SYNC_CALL, CPI_ASYNC_CALL
from   saga.cpi.async  import Async

class File (Base, Async) :

    @CPI_SYNC_CALL
    def __init__ (self, url, flags, session) : 
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



    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    @CPI_SYNC_CALL
    def is_file_self (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def get_size_self (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def read (self, size, ttype) :
        pass


    @CPI_SYNC_CALL
    def write (self, data, ttype) :
        pass


    @CPI_SYNC_CALL
    def seek (self, off, whence, ttype) :
        pass


    @CPI_SYNC_CALL
    def read_v (self, iovecs, ttype) :
        pass


    @CPI_SYNC_CALL
    def write_v (self, data, ttype) :
        pass


    @CPI_SYNC_CALL
    def size_p (self, pattern, ttype) :
        pass


    @CPI_SYNC_CALL
    def read_p (self, pattern, ttype) :
        pass


    @CPI_SYNC_CALL
    def write_p (self, pattern, data, ttype) :
        pass


    @CPI_SYNC_CALL
    def modes_e (self, ttype) :
        pass


    @CPI_SYNC_CALL
    def size_e (self, emode, spec, ttype) :
        pass


    @CPI_SYNC_CALL
    def read_e (self, emode, spec, ttype) :
        pass


    @CPI_SYNC_CALL
    def write_e (self, emode, spec, data, ttype) :
        pass
  

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

