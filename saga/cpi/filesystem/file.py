
from   saga.cpi.base   import Base
import saga.exceptions

class Service (Base) :

    def __init__ (self, url=None, flags=READ, session=None) : 
        raise saga.exceptions.NotImplemented ("method not implemented")


    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    def get_url (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_cwd (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_name (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_dir_self (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_entry_self (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_link_self (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_link_self (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def copy_self (self, tgt, flags=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def link_self (self, tgt, flags=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def move_self (self, tgt, flags=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def remove_self (self, flags=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def close (self, timeout=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def permissions_allow_self (self, id, perms, flags=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def permissions_deny_self (self, id, perms, flags=None, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")



    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    def is_file_self (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_size_self (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read (self, size=-1, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write (self, data, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def seek (self, off, whence=START, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_v (self, iovecs, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write_v (self, data, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def size_p (self, pattern, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_p (self, pattern, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write_p (self, pattern, data, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def modes_e (self, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def size_e (self, emode, spec, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_e (self, emode, spec, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def write_e (self, emode, spec, data, ttype=None) :
        raise saga.exceptions.NotImplemented ("method not implemented")
  
