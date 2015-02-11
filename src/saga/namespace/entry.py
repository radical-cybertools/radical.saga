
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils.signatures as rus

import saga.adaptors.base    as sab
import saga.exceptions       as se
import saga.session          as ss
import saga.task             as st
import saga.url              as surl
import saga.base             as sb
import saga.async            as sasync

from   saga.namespace.constants  import *
from   saga.constants            import SYNC, ASYNC, TASK


# ------------------------------------------------------------------------------
#
class Entry (sb.Base, sasync.Async) :
    '''
    Represents a SAGA namespace entry as defined in GFD.90

    The saga.namespace.Entry class represents, as the name indicates,
    an entry in some (local or remote) namespace.  That class offers
    a number of operations on that entry, such as copy, move and remove::
    
        # get an entry handle
        entry = saga.namespace.Entry ("sftp://localhost/tmp/data/data.bin")
    
        # copy the entry
        entry.copy ("sftp://localhost/tmp/data/data.bak")

        # move the entry
        entry.move ("sftp://localhost/tmp/data/data.new")
    '''

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session),
                  rus.optional (sab.Base), 
                  rus.optional (dict), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
    def __init__ (self, url=None, flags=None, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        :param url: Url of the (remote) entry
        :type  url: :class:`saga.Url` 

        flags:     flags enum
        session:   saga.Session
        ret:       obj

        Construct a new entry object

        The specified entry is expected to exist -- otherwise a DoesNotExist
        exception is raised.  Also, the URL must point to an entry (not to
        a directory), otherwise a BadParameter exception is raised.

        Example::

            # get an entry handle
            entry = saga.namespace.Entry("sftp://localhost/tmp/data/data.bin")
    
            # print the entry's url
            print entry.get_url ()
        '''

        self._session      = session
        self._is_recursive = False # recursion guard (FIXME: NOT THREAD SAFE)

        # param checks
        if not session :
            session = ss.Session (default=True)

        if  not flags : flags = 0
        url     = surl.Url (url)
        scheme  = url.scheme.lower ()

        self._base = super  (Entry, self)
        self._base.__init__ (scheme, _adaptor, _adaptor_state, 
                             url, flags, session, ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @rus.takes   ('Entry', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
    def create   (cls, url=None, flags=None, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.namespace.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        # param checks
        if  not flags : flags = 0
        if not session :
            session = ss.Session (default=True)

        return cls (url, flags, session, _ttype=ttype)._init_task



    # ----------------------------------------------------------------
    #
    # namespace entry methods
    #
    @rus.takes   ('Entry',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((surl.Url, st.Task))
    def get_url  (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        
        Return the complete url pointing to the entry.

        The call will return the complete url pointing to
        this entry as a saga.Url object::

            # print URL of an entry
            entry = saga.namespace.Entry("sftp://localhost/etc/passwd")
            print entry.get_url()
        '''
        return self._adaptor.get_url (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((basestring, st.Task))
    def get_cwd  (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           string / saga.Task
        '''
        return self._adaptor.get_cwd (ttype=ttype)
  
    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((basestring, st.Task))
    def get_name (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           string / saga.Task
        '''
        return self._adaptor.get_name (ttype=ttype)
  
    
    # ----------------------------------------------------------------
    #
    # namespace entry / directory methods
    #
    @rus.takes   ('Entry',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((bool, st.Task))
    def is_dir   (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        
        Returns True if path is a directory, False otherwise. 

        Example::

            # inspect an entry
            dir  = saga.namespace.Directory("sftp://localhost/tmp/")
            if dir.is_dir ('data'):
                # do something
        '''
        return self._adaptor.is_dir_self (ttype=ttype)
  
    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((bool, st.Task))
    def is_entry (self, ttype=None) :
        '''
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_entry_self (ttype=ttype)
  
    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((bool, st.Task))
    def is_link  (self, ttype=None) :
        '''
        tgt:           saga.Url / None
        ttype:         saga.task.type enum
        ret:           bool / saga.Task
        '''
        return self._adaptor.is_link_self (ttype=ttype)
  
    
    # --------------------------------------------------------------------------
    #
    @rus.takes    ('Entry',
                   rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns  ((surl.Url, st.Task))
    def read_link (self, ttype=None) :
        '''
        tgt:           saga.Url / None
        ttype:         saga.task.type enum
        ret:           saga.Url / saga.Task
        '''

        return self._adaptor.read_link_self (ttype=ttype)
  

    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  (surl.Url, basestring),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def copy     (self, tgt, flags=0, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         enum flags
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        
        Copy the entry to another location
    
        :param target: Url of the copy target.
        :param flags: Flags to use for the operation.
    
        The entry is copied to the given target location.  The target URL must
        be an absolute path, and can be a target entry name or target
        directory name.  If the target entry exists, it is overwritten::
    
            # copy an entry
            entry = saga.namespace.Directory("sftp://localhost/tmp/data/data.bin")
            entry.copy ("sftp://localhost/tmp/data/data.bak")
        '''
        
        # parameter checks
        if  not flags : flags = 0
        tgt_url = surl.Url (tgt)  # ensure valid and typed Url
    
    
        # async ops don't deserve a fallback (yet)
        if ttype != None :
            return self._adaptor.copy_self (tgt_url, flags, ttype=ttype)
    
    
        # we have only sync calls here - attempt a normal call to the bound
        # adaptor first (doh!)
        ret = self._adaptor.copy_self (tgt_url, flags, ttype=ttype)

        try :
            True
        
        except se.SagaException as e :
            # if we don't have a scheme for tgt, all is in vain (adaptor
            # should have handled a relative path...)
            if not tgt_url.scheme :
                raise e
    
            # So, the adaptor bound to the src URL did not manage to copy the # entry.
            # If the tgt has a scheme set, we try again with other matching # entry 
            # adaptors,  by setting (a copy of) the *src* URL to the same scheme,
            # in the hope that other adaptors can copy from localhost.
            #
            # In principle that mechanism can also be used for remote copies, but
            # URL translation is way more fragile in those cases...
        
            # check recursion guard
            if self._is_recursive :
                self._logger.debug("fallback recursion detected - abort")
              
            else :
                # activate recursion guard
                self._is_recursive += 1

                import saga.engine
                engine = saga.engine.Engine ()
    
                # find applicable adaptors we could fall back to, i.e. which
                # support the tgt schema
                adaptor_names = engine.find_adaptors ('saga.namespace.Entry', tgt_url.scheme)
    
                self._logger.debug("try fallback copy to these adaptors: %s" % adaptor_names)
    
                # build a new src url, by switching to the target schema
                tmp_url        = self.get_url ()
                tmp_url.scheme = tgt_url.scheme
    
                for adaptor_name in adaptor_names :
                  
                    try :
                        self._logger.info("try fallback copy to %s" % adaptor_name)
    
                        adaptor_instance = engine.get_adaptor (adaptor_name)
    
                        # get an tgt-scheme'd adaptor for the new src url, and try copy again
                        adaptor = engine.bind_adaptor (self, 'saga.namespace.Entry', tgt_url.scheme, 
                                                             adaptor_instance)
                        adaptor.init_instance ({}, tmp_url, None, self._session)
                        tmp     = Entry (tmp_url, None, self._session, _adaptor=adaptor_instance)
    
                        ret = tmp.copy (tgt_url, flags)
    
                        # release recursion guard
                        self._is_recursive -= 1
    
                        # if nothing raised an exception so far, we are done.
                        return 
    
    
                    except se.SagaException as e :
    
                        self._logger.info("fallback failed: %s" % e)
    
                        # didn't work, ignore this adaptor
                        pass
    
            # if all was in vain, we rethrow the original exception
            self._is_recursive -= 1
            raise e
     
    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  (surl.Url, basestring),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def link     (self, tgt, flags=0, ttype=None) :
        '''
        tgt:           saga.Url
        flags:         enum flags
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''

        if  not flags : flags = 0
        return self._adaptor.link_self (tgt, flags, ttype=ttype) 


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  (surl.Url, basestring),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def move     (self, tgt, flags=0, ttype=None) :
        '''
        :param target: Url of the move target.
        :param flags:  Flags to use for the operation.

        ttype:         saga.task.type enum
        ret:           None / saga.Task
        
        Move the entry to another location

        The entry is copied to the given target location.  The target URL must
        be an absolute path, and can be a target entry name or target
        directory name.  If the target entry exists, it is overwritten::

            # copy an entry
            entry = saga.namespace.Directory("sftp://localhost/tmp/data/data.bin")
            entry.move ("sftp://localhost/tmp/data/data.bak")
        '''
        if  not flags : flags = 0
        return self._adaptor.move_self (tgt, flags, ttype=ttype) 
  
    
    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def remove   (self, flags=0, ttype=None) :
        '''
        :param flags:  Flags to use for the operation.

        ttype:         saga.task.type enum
        ret:           None / saga.Task
        
        Reove the entry.

        The entry is removed, and this object instance is then invalid for
        further operations.

            # remove an entry
            entry = saga.namespace.Directory("sftp://localhost/tmp/data/data.bin")
            entry.remove ()
        '''
        if  not flags : flags = 0
        return self._adaptor.remove_self (flags, ttype=ttype) 
  
    
    
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Entry',
                  rus.optional (float),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def close (self, timeout=None, ttype=None) :
        '''
        timeout:       float
        ttype:         saga.task.type enum
        ret:           None / saga.Task
        '''
        return self._adaptor.close (timeout, ttype=ttype)

    
    # --------------------------------------------------------------------------
    #
    url  = property (get_url)   # saga.Url
    cwd  = property (get_cwd)   # string
    name = property (get_name)  # string




