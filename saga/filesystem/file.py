

from   saga.engine.logger import getLogger
from   saga.engine.engine import getEngine, ANY_ADAPTOR
from   saga.task          import SYNC, ASYNC, TASK
from   saga.url           import Url

import saga.exceptions
import saga.attributes

from saga.filesystem import *

# permissions.Permissions, task.Async
class File (object) :

    def __init__ (self, url=None, flags=READ, session=None) : 
        '''
        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        file_url = Url (url)

        self._engine  = getEngine ()
        self._logger  = getLogger ('saga.filesystem.File')
        self._logger.debug ("saga.filesystem.File.__init__ (%s, %s)"  \
                         % (str(file_url), str(session)))

        self._adaptor = self._engine.get_adaptor (self, 'saga.filesystem.File', file_url.scheme, \
                                                  SYNC, ANY_ADAPTOR, file_url, session)


    @classmethod
    def create (self, url=None, flags=READ, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.filesystem.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        file_url = Url (url)
    
        engine = getEngine ()
        logger = getLogger ('saga.filesystem.File.create')
        logger.debug ("saga.filesystem.File.create(%s, %s, %s)"  \
                   % (str(file_url), str(session), str(ttype)))
    
        # attempt to find a suitable adaptor, which will call 
        # init_instance_async(), which returns a task as expected.
        return engine.get_adaptor (self, 'saga.filesystem.File', file_url.scheme, \
                                   ttype, ANY_ADAPTOR, file_url, session)


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
    # filesystem methods
    #
    def is_file_self (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file_self (jd, ttype=ttype)

  
    def get_size_self (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.get_file_self (jd, ttype=ttype)

  
    def read (self, size=-1, ttype=None) :
        '''
        size :    int
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.read (size, ttype=ttype)

  
    def write (self, data, ttype=None) :
        '''
        data :    string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.write (data, ttype=ttype)

  
    def seek (self, off, whence=START, ttype=None) :
        '''
        off :     int
        whence:   seek_mode enum
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.seek (off, whence, ttype=ttype)

  
    def read_v (self, iovecs, ttype=None) :
        '''
        iovecs:   list [tuple (int, int)]
        ttype:    saga.task.type enum
        ret:      list [bytearray] / saga.Task
        '''
        return self._adaptor.read_v (iovecs, ttype=ttype)

  
    def write_v (self, data, ttype=None) :
        '''
        data:     list [tuple (int, string / bytearray)]
        ttype:    saga.task.type enum
        ret:      list [int] / saga.Task
        '''
        return self._adaptor.write_v (data, ttype=ttype)

  
    def size_p (self, pattern, ttype=None) :
        '''
        pattern:  string 
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.size_p (pattern, ttype=ttype)
  

    def read_p (self, pattern, ttype=None) :
        '''
        pattern:  string
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.read_p (pattern, ttype=ttype)

  
    def write_p (self, pattern, data, ttype=None) :
        '''
        pattern:  string
        data:     string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.write_p (pattern, data, ttype=ttype)

  
    def modes_e (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      list [string] / saga.Task
        '''
        return self._adaptor.modes_e (ttype=ttype)

  
    def size_e (self, emode, spec, ttype=None) :
        '''
        emode:    string
        spec:     string
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.size_e (emode, spec, ttype=ttype)

  
    def read_e (self, emode, spec, ttype=None) :
        '''
        emode:    string
        spec:     string
        ttype:    saga.task.type enum
        ret:      bytearray / saga.Task
        '''
        return self._adaptor.read_e (emode, spec, ttype=ttype)

  
    def write_e (self, emode, spec, data, ttype=None) :
        '''
        emode:    string
        spec:     string
        data:     string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.read_e (emode, spec, data, ttype=ttype)

  
    size  = property (get_size_self)  # int
    modes = property (modes_e)  # int
  
  
