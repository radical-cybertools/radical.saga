
from   saga.engine.logger import getLogger
from   saga.engine.engine import getEngine, ANY_ADAPTOR
from   saga.task          import SYNC, ASYNC, TASK
from   saga.url           import Url
from   saga.filesystem    import *

import saga.exceptions
import saga.attributes


# permissions.Permissions, task.Async
class Directory (object) :

    def __init__ (self, url=None, flags=READ, session=None, _adaptor=None) : 
        '''
        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        dir_url = Url (url)

        self._engine  = getEngine ()
        self._logger  = getLogger ('saga.filesystem.Directory')
        self._logger.debug ("saga.filesystem.Directory.__init__ (%s, %s)"  \
                         % (str(dir_url), str(session)))

        if _adaptor :
            # created from adaptor
            self._adaptor = _adaptor
        else :
            self._adaptor = self._engine.get_adaptor (self, 'saga.filesystem.Directory', dir_url.scheme, \
                                                      None, ANY_ADAPTOR, dir_url, flags, session)


    @classmethod
    def create (self, url=None, flags=READ, session=None, ttype=saga.task.SYNC) :
        '''
        url:       saga.Url
        flags:     saga.filesystem.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        dir_url = Url (url)
    
        engine = getEngine ()
        logger = getLogger ('saga.filesystem.Directory.create')
        logger.debug ("saga.filesystem.Directory.create(%s, %s, %s)"  \
                   % (str(dir_url), str(session), str(ttype)))
    
        # attempt to find a suitable adaptor, which will call 
        # init_instance_async(), which returns a task as expected.
        return engine.get_adaptor (self, 'saga.filesystem.Directory', dir_url.scheme, \
                                   ttype, ANY_ADAPTOR, dir_url, flags, session)



    # ----------------------------------------------------------------
    #
    # filesystem directory methods
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
        ret:      saga.filesystem.Directory / saga.Task
        '''
        return self._adaptor.open_dir (name, flags, ttype=ttype)

    
    def open (self, name, flags=READ, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.filesystem.File / saga.Task
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



