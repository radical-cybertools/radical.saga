
from   saga.cpi.base   import Base
import saga.exceptions

class File (Base) :

    def __init__ (self, url, flags, session) : 
        raise saga.exceptions.NotImplemented ("method not implemented")


    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    def get_url (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_cwd (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_name (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_dir_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_entry_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_link_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_link_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def copy_self (self, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def link_self (self, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def move_self (self, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def remove_self (self, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def close (self, timeout, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def permissions_allow_self (self, id, perms, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def permissions_deny_self (self, id, perms, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")



    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    def is_file_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_size_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read (self, size, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write (self, data, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def seek (self, off, whence, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_v (self, iovecs, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write_v (self, data, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def size_p (self, pattern, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_p (self, pattern, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write_p (self, pattern, data, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def modes_e (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def size_e (self, emode, spec, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_e (self, emode, spec, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write_e (self, emode, spec, data, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")
  

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

