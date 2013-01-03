
from   saga.task                 import SYNC, ASYNC, TASK, NOTASK
from   saga.url                  import Url
from   saga.replica.constants    import *
from   saga.base                 import Base
from   saga.async                import Async
from   saga.attributes           import Attributes

import saga.exceptions


# permissions.Permissions, task.Async
class LogicalDirectory (Base, Attributes, Async) :

    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        # param checks
        url     = Url (url)
        scheme  = url.scheme.lower ()

        Base.__init__ (self, scheme, _adaptor, _adaptor_state, 
                       url, flags, session, ttype=_ttype)


    @classmethod
    def create (cls, url=None, flags=READ, session=None, ttype=SYNC) :
        '''
        url:       saga.Url
        flags:     saga.replica.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        # param checks
        url     = Url (url)
        scheme  = url.scheme.lower ()

        return cls (url, flags, session, _ttype=ttype)._init_task


    # ----------------------------------------------------------------
    #
    # replica directory methods
    #
    def get_size (self, name, flags=None, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.get_size (name, ttype=ttype)


    def is_file (self, name, ttype=None) :
        '''
        name:     saga.Url
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file (name, ttype=ttype)

    
    def open_dir (self, name, flags=READ, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.replica.LogicalDirectory / saga.Task
        '''
        return self._adaptor.open_dir (name, flags, ttype=ttype)

    
    def open (self, name, flags=READ, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.replica.LogicalFile / saga.Task
        '''
        url = Url(name)
        return self._adaptor.open (url, flags, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # namespace directory methods
    #
    def change_dir (self, url, ttype=None) :
        '''
        url:           saga.Url
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.change_dir (url, ttype=ttype)
  
    
    def list (self, npat=".", flags=None, ttype=None) :
        '''
        npat:          string
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           list [saga.Url] / saga.Task
        '''
        return self._adaptor.list (npat, flags, ttype=ttype)
  
    
    def find (self, npat, flags=RECURSIVE, ttype=None) :
        '''
        npat:          string
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           list [saga.Url] / saga.Task
        '''
        return self._adaptor.find (npat, flags, ttype=ttype)
  
    
    def exists (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.exists (name, ttype=ttype)
  
    
    def is_dir (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_dir (name, ttype=ttype)
  
    
    def is_entry (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_entry (name, ttype=ttype)
  
    
    def is_link (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_link (name, ttype=ttype)
  
    
    def read_link (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''
        return self._adaptor.read_link (name, ttype=ttype)
  
    
    def get_num_entries (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           int / saga.Task
        '''
        return self._adaptor.get_num_entries (ttype=ttype)
  
    
    def get_entry (self, num, ttype=None) :
        '''
        num:           int 
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''
        return self._adaptor.get_entry (num, ttype=ttype)
  
    
    def copy (self, src, tgt, flags=None, ttype=None) :
        '''
        src:           saga.Url
        tgt:           saga.Url 
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.copy (src, tgt, flags, ttype=ttype)
  
    
    def link (self, src, tgt, flags=None, ttype=None) :
        '''
        src:           saga.Url
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.link (src, tgt, flags, ttype=ttype)
  
    
    def move (self, src, tgt, flags=None, ttype=None) :
        '''
        src:           saga.Url
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.move (src, tgt, flags, ttype=ttype)
  
    
    def remove (self, tgt, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.remove (tgt, flags, ttype=ttype)
  
    
    def make_dir (self, tgt, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.mkdir (tgt, flags, ttype=ttype)
  
    
    def permissions_allow (self, tgt, id, perms, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        id:            string
        perms:         saga.permissions.flags enum
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.permissions_allow (tgt, id, perms, flags, ttype=ttype)
  
    
    def permissions_deny (self, tgt, id, perms, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        id:            string
        perms:         saga.permission.flags enum
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.permissions_deny (tgt, id, perms, flags, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    def get_url (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''
        return self._adaptor.get_url (ttype=ttype)

  
    def get_cwd (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           string / saga.Task
        '''
        return self._adaptor.get_cwd (ttype=ttype)
  
    
    def get_name (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           string / saga.Task
        '''
        return self._adaptor.get_name (ttype=ttype)
  
    
    def is_dir_self (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_dir_self (ttype=ttype)
  
    
    def is_entry_self (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_entry_self (ttype=ttype)
  
    
    def is_link_self (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_link_self (ttype=ttype)
  
    
    def read_link_self (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''
        return self._adaptor.read_link_self (ttype=ttype)
  
    
    def copy_self (self, tgt, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         enum flags
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.copy_self (tgt, flags, ttype=ttype)
  
    
    def link_self (self, tgt, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         enum flags
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.link_self (tgt, flags, ttype=ttype)
  
    
    def move_self (self, tgt, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.move_self (tgt, flags, ttype=ttype)
  
    
    def remove_self (self, flags=None, ttype=None) :
        '''
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.remove_self (flags, ttype=ttype)
  
    
    def close (self, timeout=None, ttype=None) :
        '''
        timeout:       float
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.close (timeout, ttype=ttype)
  
    
    def permissions_allow_self (self, id, perms, flags=None, ttype=None) :
        '''
        id:            string
        perms:         saga.permissions.flags enum
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.permissions_allow_self (id, perms, flags, ttype=ttype)
  
    
    def permissions_deny_self (self, id, perms, flags=None, ttype=None) :
        '''
        id:            string
        perms:         saga.permissions.flags enum
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.permissions_deny_self (id, perms, flags, ttype=ttype)
  
  
    url  = property (get_url)   # saga.Url
    cwd  = property (get_cwd)   # string
    name = property (get_name)  # string



    # ----------------------------------------------------------------
    #
    # replica methods
    #
    def is_file (self, tgt, ttype=None) :
        '''
        tgt:      Url
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file (tgt, ttype=ttype)

  
    def is_file_self (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file_self (ttype=ttype)

  
    def open_dir (self, name, flags=READ, ttype=None) :
        '''
        name:           saga.url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            Directory / saga.Task
        '''
        return self._adaptor.open_dir (name, flags, ttype=ttype)


    def open (self, name, flags=READ, ttype=None) :
        '''
        name:           saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            LogicalFile / saga.Task
        '''
        return self._adaptor.open (name, flags, ttype=ttype)


    def find (self, name_pattern, attr_pattern, flags=RECURSIVE, ttype=None) :
        '''
        name_pattern:   string 
        attr_pattern:   string
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            list [saga.Url] / saga.Task
        '''
        return self._adaptor.find (name_pattern, attr_pattern, flags, ttype=ttype)

    
