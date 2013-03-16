
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.url
import saga.exceptions
import saga.namespace.entry

from   saga.filesystem.constants import *

class File (saga.namespace.entry.Entry) :
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


    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        :param url: Url of the (remote) file
        :type  url: :class:`bliss.saga.Url` 

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
        url = saga.url.Url (url)

        self._nsentry = super  (File, self)
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

        _nsentry = super (File, cls)
        return _nsentry.create (url, flags, session, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # filesystem methods
    #
    def is_file (self, ttype=None) :
        '''
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file (ttype=ttype)

  
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
        return self._adaptor.get_size (ttype=ttype)

  
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

  
    size  = property (get_size)  # int
    modes = property (modes_e)   # list [string]
  
  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

