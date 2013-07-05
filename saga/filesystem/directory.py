
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.utils.signatures     as sus
import saga.adaptors.base        as sab
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.namespace.directory  as nsdir

from   saga.filesystem.constants import *
from   saga.constants            import SYNC, ASYNC, TASK


# ------------------------------------------------------------------------------
#
class Directory (nsdir.Directory) :
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

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Directory', 
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
        url = surl.Url (url)

        self._nsdirec = super  (Directory, self)
        self._nsdirec.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @sus.takes   ('Directory', 
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

        _nsdir = super (Directory, cls)
        return _nsdir.create (url, flags, session, ttype=ttype)

    # ----------------------------------------------------------------
    #
    # filesystem directory methods
    #
    @sus.takes   ('Directory', 
                  (surl.Url, basestring),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (('File', st.Task))
    def open (self, path, flags=READ, ttype=None) :
        '''
        path:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.namespace.Entry / saga.Task
        '''
        url = surl.Url(path)
        return self._adaptor.open (url, flags, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Directory', 
                  (surl.Url, basestring),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (('Directory', st.Task))
    def open_dir (self, path, flags=READ, ttype=None) :
        '''
        :param path: name/path of the directory to open
        :param flags: directory creation flags

        ttype:    saga.task.type enum
        ret:      saga.namespace.Directory / saga.Task
        
        Open and return a new directoy

           The call opens and returns a directory at the given location.

           Example::

               # create a subdir 'data' in /tmp
               dir = saga.namespace.Directory("sftp://localhost/tmp/")
               data = dir.open_dir ('data/', saga.namespace.Create)
        '''
        return self._adaptor.open_dir (path, flags, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Directory', 
                  sus.optional ((surl.Url, basestring)),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
    def get_size (self, tgt=None, flags=None, ttype=None) :
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


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Directory', 
                  sus.optional ((surl.Url, basestring)),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((bool, st.Task))
    def is_file (self, tgt=None, ttype=None) :
        '''
        tgt:      saga.Url
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        if tgt    :  return self._adaptor.is_file      (tgt, ttype=ttype)
        else      :  return self._adaptor.is_file_self (     ttype=ttype)


    size  = property (get_size)  # int

    
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

