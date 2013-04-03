
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.url
import saga.exceptions
import saga.namespace.entry
import saga.attributes as sa

from   saga.advert.constants import *


# keep order of inheritance!  super() below uses MRO
class Entry (saga.namespace.entry.Entry,
             saga.attributes.Attributes) :


    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        # param checks
        url = saga.url.Url (url)

        self._nsentry = super  (Entry, self)
        self._nsentry.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)


        # set attribute interface properties
        self._attributes_allow_private (True)
        self._attributes_camelcasing   (True)
        self._attributes_extensible    (True, getter=self._attribute_getter, 
                                              setter=self._attribute_setter,
                                              lister=self._attribute_lister,
                                              caller=self._attribute_caller)

        # register properties with the attribute interface 
        self._attributes_register   (ATTRIBUTE, None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register   (OBJECT,    None, sa.ANY,    sa.SCALAR, sa.READONLY)
        self._attributes_register   (TTL,       None, sa.INT,    sa.SCALAR, sa.WRITEABLE)

        self._attributes_set_setter (TTL,    self.set_ttl)
        self._attributes_set_getter (TTL,    self.get_ttl)

        self._attributes_set_setter (OBJECT, self.store_object)
        self._attributes_set_getter (OBJECT, self.retrieve_object)



    @classmethod
    def create (cls, url=None, flags=READ, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.advert.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        _nsentry = super (Entry, cls)
        return _nsentry.create (url, flags, session, ttype=ttype)



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
    # advert methods
    #
    def set_ttl (self, ttl, ttype=None) : 
        """
        ttl :           int
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        """
        return self._adaptor.set_ttl (ttl, ttype=ttype)

  
    def get_ttl (self, ttype=None) : 
        """
        ttype:          saga.task.type enum
        ret:            int / saga.Task
        """
        return self._adaptor.get_ttl (ttype=ttype)

     
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

