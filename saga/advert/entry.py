

from   saga.task                 import SYNC, ASYNC, TASK
from   saga.url                  import Url
from   saga.advert.constants import *
from   saga.base                 import Base
from   saga.async                import Async
from   saga.attributes           import *

import saga.exceptions


class Entry (Base, Attributes, Async) :


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

        # set attribute interface properties
        self._attributes_allow_private (True)
        self._attributes_camelcasing   (True)
        self._attributes_extensible    (True, getter=self._attribute_getter, 
                                              setter=self._attribute_setter,
                                              lister=self._attribute_lister,
                                              caller=self._attribute_caller)

        # register properties with the attribute interface 
        self._attributes_register   (ATTRIBUTE, None, STRING, SCALAR, READONLY)
        self._attributes_register   (OBJECT,    None, ANY,    SCALAR, READONLY)
        self._attributes_register   (EXPIRES,   None, STRING, SCALAR, READONLY)
        self._attributes_register   (TTL,       None, INT,    SCALAR, WRITEABLE)

        self._attributes_set_setter (TTL,    self.set_ttl_self)
        self._attributes_set_getter (TTL,    self.get_ttl_self)

        self._attributes_set_setter (OBJECT, self.store_object)
        self._attributes_set_getter (OBJECT, self.retrieve_object)



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
    def _attribute_getter (self, key, ttype=None) :
        return self._adaptor.attribute_getter (key)

    def _attribute_setter (self, key, val, ttype=None) :
        return self._adaptor.attribute_setter (key, val)

    def _attribute_lister (self, ttype=None) :
        return self._adaptor.key_lister ()

    def _attribute_caller (self, key, id, cb, ttype=None) :
        return self._adaptor.attribute_caller (key, id, cb)



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

     
    def store_object (self, object, ttype=None) : 
        """
        object :        <object type>
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        """
        return self._adaptor.store_object (object, ttype=ttype)

  
    def retrieve_object (self, ttype=None) : 
        """
        ttype:          saga.task.type enum
        ret:            any / saga.Task
        """
        return self._adaptor.retrieve_object (ttype=ttype)

     
    def delete_object (self, ttype=None) : 
        """
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        """
        return self._adaptor.delete_object (ttype=ttype)

     
  
  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

