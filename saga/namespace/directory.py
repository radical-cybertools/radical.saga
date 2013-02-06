
import saga.url
import saga.base
import saga.async
import saga.exceptions

from   saga.namespace.constants import *

import entry


class Directory (entry.Entry) :
    '''
    Represents a SAGA directory as defined in GFD.90
    
    The saga.namespace.Directory class represents, as the name indicates,
    a directory on some (local or remote) namespace.  That class offers
    a number of operations on that directory, such as listing its contents,
    copying entries, or creating subdirectories::
    
        # get a directory handle
        dir = saga.namespace.Directory("sftp://localhost/tmp/")
    
        # create a subdir
        dir.make_dir ("data/")
    
        # list contents of the directory
        entries = dir.list ()
    
        # copy *.dat entries into the subdir
        for f in entries :
            if f ^ '^.*\.dat$' :
                dir.copy (f, "sftp://localhost/tmp/data/")
    '''

    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        :param url: Url of the (remote) entry system directory.
        :type  url: :class:`saga.Url` 

        flags:     flags enum
        session:   saga.Session
        ret:       obj
        
        Construct a new directory object

        The specified directory is expected to exist -- otherwise
        a DoesNotExist exception is raised.  Also, the URL must point to
        a directory (not to an entry), otherwise a BadParameter exception is
        raised.

        Example::

            # open some directory
            dir = saga.namespace.Directory("sftp://localhost/tmp/")

            # and list its contents
            entries = dir.list ()

        '''

        # FIXME: param checks

        entry.Entry.__init__ (self, url, flags, session, 
                              _adaptor, _adaptor_state, _ttype=_ttype)


    @classmethod
    def create (cls, url=None, flags=READ, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.namespace.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        # param checks
        url     = saga.url.Url (url)
        scheme  = url.scheme.lower ()

        return cls (url, flags, session, _ttype=ttype)._init_task


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
        return self._adaptor.open_dir (name, flags, ttype=ttype)

    
    def open (self, name, flags=READ, ttype=None) :
        '''
        name:     saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.namespace.Entry / saga.Task
        '''
        url = saga.url.Url(name)
        return self._adaptor.open (url, flags, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # namespace directory methods
    #
    def change_dir (self, url, ttype=None) :
        '''
        url:           saga.Url
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.change_dir (url, ttype=ttype)
  
    
    def list (self, npat=".", flags=None, ttype=None) :
        '''
        :param npat: Entry name pattern (like POSIX 'ls', e.g. '\*.txt')

        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           list [saga.Url] / saga.Task
        
        List the directory's content

        The call will return a list of entries and subdirectories within the
        directory::

            # list contents of the directory
            for f in dir.list() :
                print f
        '''
        return self._adaptor.list (npat, flags, ttype=ttype)
  
    
    def find (self, npat, flags=RECURSIVE, ttype=None) :
        '''
        npat:          string
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           list [saga.Url] / saga.Task
        '''
        return self._adaptor.find (npat, flags, ttype=ttype)
  
    
    def exists (self, path, ttype=None) :

        '''
        :param path: path of the entry to check

        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        
        Returns True if path exists, False otherwise. 


        Example::

            # inspect an entry
            dir  = saga.namespace.Directory("sftp://localhost/tmp/")
            if dir.exists ('data'):
                # do something
        '''
    
        return self._adaptor.exists (path, ttype=ttype)
  
    
    def is_dir (self, path, ttype=None) :
        '''
        :param path: path of the entry to check

        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        
        Returns True if path is a directory, False otherwise. 

        Example::

            # inspect an entry
            dir  = saga.namespace.Directory("sftp://localhost/tmp/")
            if dir.is_dir ('data'):
                # do something
        '''
        return self._adaptor.is_dir (name, ttype=ttype)
  
    
    def is_entry (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_entry (name, ttype=ttype)
  
    
    def is_link (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_link (name, ttype=ttype)
  
    
    def read_link (self, name, ttype=None) :
        '''
        name:          saga.Url
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''
        return self._adaptor.read_link (name, ttype=ttype)
  
    
    def get_num_entries (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           int / saga.Task
        '''
        return self._adaptor.get_num_entries (ttype=ttype)
  
    
    def get_entry (self, num, ttype=None) :
        '''
        num:           int 
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''
        return self._adaptor.get_entry (num, ttype=ttype)
  
    
    def copy (self, url_1, url_2=None, flags=None, ttype=None) :
        '''
        :param src: path of the entry to copy
        :param tgt: absolute URL of target name or directory
        
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        
        Copy an entry from source to target

        The source is copied to the given target directory.  The path of the
        source can be relative::

            # copy an entry
            dir = saga.namespace.Directory("sftp://localhost/tmp/")
            dir.copy ("./data.bin", "sftp://localhost/tmp/data/")
        '''

        if  url_1 and url_2 :
            return self._adaptor.copy      (url_1, url_2, flags, ttype=ttype)

        if  url_1 :
            return self._adaptor.copy_self (url_1,        flags, ttype=ttype)
    
        raise BadParameter ("signature mismatch: copy (url, [url], flags, ttype)")

      # return self._adaptor.copy (src, tgt, flags, ttype=ttype)


  
    
    def link (self, src, tgt, flags=None, ttype=None) :
        '''
        src:           saga.Url
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.link (src, tgt, flags, ttype=ttype)
  
    
    def move (self, src, tgt, flags=None, ttype=None) :
        '''
        :param src: path of the entry to copy
        :param tgt: absolute URL of target directory

        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        
        Move an entry from source to target

        The source is moved to the given target directory.  The path of the
        source can be relative::

            # copy an entry
            dir = saga.namespace.Directory("sftp://localhost/tmp/")
            dir.move ("./data.bin", "sftp://localhost/tmp/data/")

        '''
        return self._adaptor.move (src, tgt, flags, ttype=ttype)
  
    
    def remove (self, tgt, flags=None, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         flags enum
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.remove (tgt, flags, ttype=ttype)
  
    
    def make_dir (self, tgt, flags=None, ttype=None) :
        '''
        :param tgt:   name/path of the new directory
        :param flags: directory creation flags

        ttype:         saga.task.type enum
        ret:           None / saga.Task
        
        Create a new directoy

        The call creates a directory at the given location.

        Example::

            # create a subdir 'data' in /tmp
            dir = saga.namespace.Directory("sftp://localhost/tmp/")
            dir.make_dir ('data/')
        '''

        return self._adaptor.make_dir (tgt, flags, ttype=ttype)
  
    
    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    def get_url (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        
        Return the complete url pointing to the directory.

        The call will return the complete url pointing to
        this directory as a saga.Url object::

            # print URL of a directory
            dir = saga.namespace.Directory("sftp://localhost/tmp/")
            print dir.get_url()
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
        
        Removes the directory

        If no path is given, the remote directory associated with
        the object is removed. If a relative or absolute path is given,
        that given target is removed instead.  The target must be a 
        directory.

        :param path: (relative or absolute) path to a directory

       
        Example::

            # remove a subdir 'data' in /tmp
            dir = saga.namespace.Directory("sftp://localhost/tmp/")
            dir.remove ('data/')
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



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

