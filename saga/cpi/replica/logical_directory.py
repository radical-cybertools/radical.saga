
from   saga.cpi.base   import Base
import saga.exceptions

class LogicalDirectory (Base) :

    def __init__ (self, url, flags, session) : 
        raise saga.exceptions.NotImplemented ("method not implemented")


    # ----------------------------------------------------------------
    #
    # replica directory methods
    #
    def is_file (self, name, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def open_dir (self, name, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def open (self, name, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    # ----------------------------------------------------------------
    #
    # namespace directory methods
    #
    def change_dir (self, url, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def list (self, npat, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def find (self, npat, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def exists (self, name, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_dir (self, name, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_entry (self, name, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def is_link (self, name, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def read_link (self, name, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_num_entries (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def get_entry (self, num, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def copy (self, src, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def link (self, src, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def move (self, src, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def remove (self, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def make_dir (self, tgt, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def permissions_allow (self, tgt, id, perms, flags, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def permissions_deny (self, tgt, id, perms, flags, ttype) :
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


