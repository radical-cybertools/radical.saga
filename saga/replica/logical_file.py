
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.url
import saga.exceptions
import saga.namespace.entry
import saga.attributes

from   saga.replica.constants import *


# keep order of inheritance!  super() below uses MRO
class LogicalFile (saga.namespace.entry.Entry,
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

        self._nsentry = super  (LogicalFile, self)
        self._nsentry.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)


    @classmethod
    def create (cls, url=None, flags=READ, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.replica.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        _nsentry = super (LogicalFile, cls)
        return _nsentry.create (url, flags, session, ttype=ttype)



    # ----------------------------------------------------------------
    #
    # replica methods
    #
    def add_location (self, name, ttype=None) :
        '''
        name:           saga.Url
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.add_location (name, ttype=ttype)


    def remove_location (self, name, ttype=None) :
        '''
        name:           saga.Url
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.remove_location (name, ttype=ttype)


    def update_location (self, old, new, ttype=None) :
        '''
        old:            saga.Url
        new:            saga.Url 
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.update_location (old, new, ttype=ttype)


    def list_locations (self, ttype=None) :
        '''
        ttype:          saga.task.type enum
        ret:            list [saga.Url] / saga.Task
        '''
        return self._adaptor.list_locations (ttype=ttype)


    def replicate (self, name, flags=None, ttype=None) :
        '''
        name:           saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.replicate (name, flags, ttype=ttype)
    

    def upload (self, name, tgt="", flags=None, ttype=None) :
        '''
        name:           saga.Url
        tgt:            saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.upload (name, tgt, flags, ttype=ttype)
    
  
    def download (self, name, src="", flags=None, ttype=None) :
        '''
        name:           saga.Url
        src:            saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.download (name, src, flags, ttype=ttype)
    
  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

