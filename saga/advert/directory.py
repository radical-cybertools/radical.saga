
from   saga.task                 import SYNC, ASYNC, TASK
from   saga.url                  import Url
from   saga.advert.constants     import *
from   saga.base                 import Base
from   saga.async                import Async
from   saga.attributes           import *

import saga.exceptions


class Directory (Base, Attributes, Async) :

    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        # param checks
        print url
        url     = Url (url)
        print url
        scheme  = url.scheme.lower ()

        print "schema: %s" % scheme
        Base.__init__ (self, scheme, _adaptor, _adaptor_state, 
                       url, flags, session, ttype=_ttype)

        Attributes.__init__ (self, ttype=_ttype)


        # set attribute interface properties
        self._attributes_allow_private (True)
        self._attributes_camelcasing   (True)
        self._attributes_extensible    (True, getter=self._attribute_getter, 
                                              setter=self._attribute_setter,
                                              lister=self._attribute_lister,
                                              caller=self._attribute_caller)

        # register properties with the attribute interface 
        self._attributes_register   (ATTRIBUTE, None, STRING, SCALAR, READONLY)
        self._attributes_register   (CHANGE,    None, STRING, SCALAR, READONLY)
        self._attributes_register   (NEW,       None, STRING, SCALAR, READONLY)
        self._attributes_register   (DELETE,    None, STRING, SCALAR, READONLY)
        self._attributes_register   (EXPIRES,   None, STRING, SCALAR, READONLY)
        self._attributes_register   (TTL,       None, INT,    SCALAR, WRITEABLE)

        self._attributes_set_setter (TTL, self.set_ttl_self)
        self._attributes_set_getter (TTL, self.get_ttl_self)



    @classmethod
    def create (cls, url=None, flags=READ, session=None, ttype=SYNC) :
        '''
        url:       saga.Url
        flags:     saga.advert.flags enum
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
    # attribute methods
    #
    # NOTE: we do not yet pass ttype, as async calls are not yet supported
    #
    def _attribute_getter (self, key, ttype=None) :
        return self._adaptor.attribute_getter (key)

    def _attribute_setter (self, key, val, ttype=None) :
        return self._adaptor.attribute_setter (key, val)

    def _attribute_lister (self, ttype=None) :
        return self._adaptor.attribute_lister ()

    def _attribute_caller (self, key, id, cb, ttype=None) :
        return self._adaptor.attribute_caller (key, id, cb)



    # ----------------------------------------------------------------
    #
    # advert directory methods
    #
    def open_dir (self, name, flags=READ, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.advert.Directory / saga.Task
        '''
        return self._adaptor.open_dir (name, flags, ttype=ttype)

    
    def open (self, name, flags=READ, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.advert.Entry/ saga.Task
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
  
  
    url  = property (get_url)   # saga.Url
    cwd  = property (get_cwd)   # string
    name = property (get_name)  # string


    # ----------------------------------------------------------------
    #
    # advert methods
    #
    def set_ttl_self (self, ttl, ttype=None) : 
        """
        ttl :           int
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        """
        return self._adaptor.set_ttl_self (ttl, ttype=ttype)


    def get_ttl_self (self, ttype=None) : 
        """
        ttype:          saga.task.type enum
        ret:            int / saga.Task
        """
        return self._adaptor.get_ttl_self (ttype=ttype)

     
    def set_ttl (self, tgt, ttl, ttype=None) : 
        """
        tgt :           saga.Url
        ttl :           int
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        """
        return self._adaptor.set_ttl (ttl, ttype=ttype)

     
    def get_ttl (self, tgt, ttype=None) : 
        """
        tgt :           saga.Url
        ttype:          saga.task.type enum
        ret:            int / saga.Task
        """
        return self._adaptor.get_ttl (ttype=ttype)


    def find_adverts (self, name_pattern, attr_pattern, obj_type,
                      flags=RECURSIVE, ttype=None) : 
        """
        name_pattern:   string
        attr_pattern:   string
        obj_type:       string
        flags:          flags enum
        ret:            list [saga.Url]
        """
        return self._adaptor.find_adverts (name_pattern, attr_pattern, obj_type, flags, ttype=ttype)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

