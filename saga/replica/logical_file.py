
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.utils.signatures     as sus
import saga.adaptors.base        as sab
import saga.attributes           as sa
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.namespace.entry      as nsentry

from   saga.filesystem.constants import *
from   saga.constants            import SYNC, ASYNC, TASK


# ------------------------------------------------------------------------------
#
class LogicalFile (nsentry.Entry, sa.Attributes) :

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        # param checks
        url = surl.Url (url)

        self._nsentry = super  (LogicalFile, self)
        self._nsentry.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (st.Task)
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
    @sus.takes   ('LogicalFile', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((bool, st.Task))
    def is_file (self, ttype=None) :
        '''
        ttype:          saga.task.type enum
        ret:            bool / saga.Task
        '''
        return self.is_entry (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    # replica methods
    #
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def add_location (self, name, ttype=None) :
        '''
        name:           saga.Url
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.add_location (name, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def remove_location (self, name, ttype=None) :
        '''
        name:           saga.Url
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.remove_location (name, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def update_location (self, old, new, ttype=None) :
        '''
        old:            saga.Url
        new:            saga.Url 
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.update_location (old, new, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('LogicalFile', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (surl.Url), st.Task))
    def list_locations (self, ttype=None) :
        '''
        ttype:          saga.task.type enum
        ret:            list [saga.Url] / saga.Task
        '''
        return self._adaptor.list_locations (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def replicate (self, name, flags=None, ttype=None) :
        '''
        name:           saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.replicate (name, flags, ttype=ttype)
    

    # --------------------------------------------------------------------------
    # non-GFD.90
    #
    @sus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  sus.optional ((surl.Url, basestring)),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def upload (self, name, tgt=None, flags=None, ttype=None) :
        '''
        name:           saga.Url
        tgt:            saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.upload (name, tgt, flags, ttype=ttype)
    
 
    # --------------------------------------------------------------------------
    # non-GFD.90
    #
    @sus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  sus.optional ((surl.Url, basestring)),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def download (self, name, src=None, flags=None, ttype=None) :
        '''
        name:           saga.Url
        src:            saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.download (name, src, flags, ttype=ttype)
    
  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

