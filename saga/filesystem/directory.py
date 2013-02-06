
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

        saga.namespace.directory.Directory.__init__ (self, url, flags, session,
                                                     _adaptor, _adaptor_state, _ttype=_ttype)


    # ----------------------------------------------------------------
    #
    # filesystem directory methods
    #
    def get_size (self, path, flags=None, ttype=None) :
        '''
        :param path: path of the file or directory

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
        return self._adaptor.get_size (name, ttype=ttype)


    def is_file (self, name, ttype=None) :
        '''
        name:     saga.Url
        ttype:    saga.task.type enum
        ret:      bool / saga.Task
        '''
        return self._adaptor.is_file (name, ttype=ttype)

    
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

