
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.url
import saga.exceptions
import saga.namespace.directory

from   saga.filesystem.constants import *

class Directory (saga.namespace.directory.Directory) :
    '''
    Represents a SAGA directory as defined in GFD.90
    
    The saga.filesystem.Directory class represents, as the name indicates,
    a directory on some (local or remote) filesystem.  That class offers
    a number of operations on that directory, such as listing its contents,
    copying files, or creating subdirectories::
    
        # get a directory handle
        dir = saga.filesystem.Directory("sftp://localhost/tmp/")
    
        # create a subdir
        dir.make_dir ("data/")
    
        # list contents of the directory
        files = dir.list ()
    
        # copy *.dat files into the subdir
        for f in files :
            if f ^ '^.*\.dat$' :
                dir.copy (f, "sftp://localhost/tmp/data/")
    '''

    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        :param url: Url of the (remote) file system directory.
        :type  url: :class:`saga.Url` 

        flags:     flags enum
        session:   saga.Session
        ret:       obj
        
        Construct a new directory object

        The specified directory is expected to exist -- otherwise
        a DoesNotExist exception is raised.  Also, the URL must point to
        a directory (not to a file), otherwise a BadParameter exception is
        raised.

        Example::

            # open some directory
            dir = saga.filesystem.Directory("sftp://localhost/tmp/")

            # and list its contents
            files = dir.list ()

        '''

        # param checks
        url = saga.url.Url (url)

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

        _nsdir = super (Directory, cls)
        return _nsdir.create (url, flags, session, ttype=ttype)

    # ----------------------------------------------------------------
    #
    # filesystem directory methods
    #
    def get_size (self, tgt, flags=None, ttype=None) :
        '''
        :param tgt: path of the file or directory

        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      int / saga.Task
        
        Returns the size of a file or directory (in bytes)

        Example::

            # inspect a file for its size
            dir  = saga.filesystem.Directory("sftp://localhost/tmp/")
            size = dir.get_size ('data/data.bin')
            print size
        '''
        if tgt    :  return self._adaptor.get_size      (tgt, ttype=ttype)
        else      :  return self._adaptor.get_size_self (     ttype=ttype)


    def is_file (self, tgt=None, ttype=None) :
        '''
        tgt:      saga.Url
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        if tgt    :  return self._adaptor.is_file      (tgt, ttype=ttype)
        else      :  return self._adaptor.is_file_self (     ttype=ttype)

    
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

