
import saga.url
import saga.exceptions
import saga.namespace.directory
import saga.attributes

from   saga.replica.constants import *

# keep order of inheritance!  super() below uses MRO
class LogicalDirectory (saga.namespace.directory.Directory, 
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
        url = Url (url)

        self._nsdirec = super  (Directory, self)
        self._nsdirec.__init__ (url, flags, session, 
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

        # param checks
        url     = Url (url)
        scheme  = url.scheme.lower ()

        return cls (url, flags, session, _ttype=ttype)._init_task


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    def find (self, name_pattern, attr_pattern=None, flags=RECURSIVE, ttype=None) :
        '''
        name_pattern:   string 
        attr_pattern:   string
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            list [saga.Url] / saga.Task

        '''
        if attr_pattern  :  return self._adaptor.find_replicas (name_pattern, attr_pattern, flags, ttype=ttype)
        else             :  return self._nsdirec.find          (name_pattern,               flags, ttype=ttype)

    
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

