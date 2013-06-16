
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.utils.signatures     as sus
import saga.adaptors.base        as sab
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.namespace.entry      as nsentry

from   saga.filesystem.constants import *
from   saga.constants            import SYNC, ASYNC, TASK

# ------------------------------------------------------------------------------
#
class File (nsentry.Entry) :
    '''
    Represents a SAGA file as defined in GFD.90

    The saga.filesystem.File class represents, as the name indicates,
    a file on some (local or remote) filesystem.  That class offers
    a number of operations on that file, such as copy, move and remove::
    
        # get a file handle
        file = saga.filesystem.File("sftp://localhost/tmp/data/data.bin")
    
        # copy the file
        file.copy ("sftp://localhost/tmp/data/data.bak")

        # move the file
        file.move ("sftp://localhost/tmp/data/data.new")
    '''

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
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
        :param url: Url of the (remote) file
        :type  url: :class:`saga.Url` 

        flags:     flags enum
        session:   saga.Session
        ret:       obj

        Construct a new file object

        The specified file is expected to exist -- otherwise a DoesNotExist
        exception is raised.  Also, the URL must point to a file (not to
        a directory), otherwise a BadParameter exception is raised.

        Example::

            # get a file handle
            file = saga.filesystem.File("sftp://localhost/tmp/data/data.bin")
    
            # print the file's size
            print file.get_size ()
        '''

        # param checks
        url = surl.Url (url)

        self._nsentry = super  (File, self)
        self._nsentry.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)

    # --------------------------------------------------------------------------
    #
    @classmethod
    @sus.takes   ('File', 
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

        _nsentry = super (File, cls)
        return _nsentry.create (url, flags, session, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((bool, st.Task))
    def is_file (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file_self (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def get_size (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        
        Returns the size of a file (in bytes)

           Example::

               # get a file handle
               file = saga.filesystem.File("sftp://localhost/tmp/data/data.bin")
    
               # print the file's size
               print file.get_size ()

        '''
        return self._adaptor.get_size_self (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
    def read     (self, size=None, ttype=None) :
        '''
        size :    int
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.read (size, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def write    (self, data, ttype=None) :
        '''
        data :    string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.write (data, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  int,
                  sus.optional (sus.one_of (START, CURRENT, END )),
                  sus.optional (sus.one_of (SYNC,  ASYNC,   TASK)))
    @sus.returns ((int, st.Task))
    def seek     (self, offset, whence=START, ttype=None) :
        '''
        offset:   int
        whence:   seek_mode enum
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.seek (offset, whence, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  sus.list_of  (sus.tuple_of (int)),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
    def read_v   (self, iovecs, ttype=None) :
        '''
        iovecs:   list [tuple (int, int)]
        ttype:    saga.task.type enum
        ret:      list [bytearray] / saga.Task
        '''
        return self._adaptor.read_v (iovecs, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  sus.list_of  (sus.tuple_of ((int, basestring))),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (int), st.Task))
    def write_v (self, data, ttype=None) :
        '''
        data:     list [tuple (int, string / bytearray)]
        ttype:    saga.task.type enum
        ret:      list [int] / saga.Task
        '''
        return self._adaptor.write_v (data, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def size_p (self, pattern, ttype=None) :
        '''
        pattern:  string 
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.size_p (pattern, ttype=ttype)
  

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
    def read_p (self, pattern, ttype=None) :
        '''
        pattern:  string
        ttype:    saga.task.type enum
        ret:      string / bytearray / saga.Task
        '''
        return self._adaptor.read_p (pattern, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def write_p (self, pattern, data, ttype=None) :
        '''
        pattern:  string
        data:     string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.write_p (pattern, data, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (basestring), st.Task))
    def modes_e (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      list [string] / saga.Task
        '''
        return self._adaptor.modes_e (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def size_e (self, emode, spec, ttype=None) :
        '''
        emode:    string
        spec:     string
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.size_e (emode, spec, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((basestring, st.Task))
    def read_e (self, emode, spec, ttype=None) :
        '''
        emode:    string
        spec:     string
        ttype:    saga.task.type enum
        ret:      bytearray / saga.Task
        '''
        return self._adaptor.read_e (emode, spec, ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @sus.takes   ('File', 
                  basestring,
                  basestring,
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def write_e (self, emode, spec, data, ttype=None) :
        '''
        emode:    string
        spec:     string
        data:     string / bytearray
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        '''
        return self._adaptor.read_e (emode, spec, data, ttype=ttype)

  
    size    = property (get_size)  # int
    modes_e = property (modes_e)   # list [string]
  
  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

