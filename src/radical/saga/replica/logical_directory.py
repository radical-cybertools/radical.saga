
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils              as ru
import radical.utils.signatures   as rus

from ..constants import SYNC, ASYNC, TASK
from ..adaptors  import base      as sab
from ..namespace import directory as nsdir

from .. import attributes         as sa
from .. import session            as ss
from .. import task               as st
from .  import constants          as c


# ------------------------------------------------------------------------------
#
class LogicalDirectory (nsdir.Directory, sa.Attributes) :

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('LogicalDirectory',
                  rus.optional ((ru.Url, str)),
                  rus.optional (int, rus.nothing),
                  rus.optional (ss.Session),
                  rus.optional (sab.Base),
                  rus.optional (dict),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
    def __init__ (self, url=None, flags=c.READ, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) :
        '''
        __init__(url, flags=READ, session=None)

        Create a new Logical Directory instance.

        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        # param checks
        if not flags : flags = 0
        url = ru.Url (url)

        self._nsdirec = super  (LogicalDirectory, self)
        self._nsdirec.__init__ (url, flags, session,
                                _adaptor, _adaptor_state, _ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @rus.takes   ('LogicalDirectory',
                  rus.one_of (ru.Url, str),
                  rus.optional (int, rus.nothing),
                  rus.optional (ss.Session),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
    def create (cls, url, flags=c.READ, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.replica.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

        if not flags : flags = 0
        _nsdirec = super (LogicalDirectory, cls)
        return _nsdirec.create (url, flags, session, ttype=ttype)


    @rus.takes   ('LogicalDirectory',
                  rus.one_of (ru.Url, str),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((bool, st.Task))
    def is_file (self, tgt=None, ttype=None) :
        '''
        is_file(tgt=None)

        tgt:           saga.Url / string
        ttype:          saga.task.type enum
        ret:            bool / saga.Task
        '''
        if tgt: return self._adaptor.is_file      (tgt, ttype=ttype)
        else  : return self._nsdirec.is_entry_self(     ttype=ttype)


    @rus.takes   ('LogicalDirectory',
                  rus.one_of (ru.Url, str),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (('LogicalFile', st.Task))
    def open (self, tgt, flags=c.READ, ttype=None) :
        '''
        open(tgt, flags=READ)

        tgt:      saga.Url
        flags:    saga.namespace.flags enum
        ttype:    saga.task.type enum
        ret:      saga.namespace.Entry / saga.Task
        '''
        if not flags : flags = 0
        tgt_url = ru.Url (tgt)
        return self._adaptor.open (tgt_url, flags, ttype=ttype)


    # ----------------------------------------------------------------
    #
    @rus.takes   ('LogicalDirectory',
                  rus.one_of (ru.Url, str),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (('LogicalDirectory', st.Task))
    def open_dir (self, tgt, flags=c.READ, ttype=None) :
        '''
        open_dir(tgt, flags=READ)

        :param tgt:   name/path of the directory to open
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
        if not flags : flags = 0
        tgt_url = ru.Url (tgt)
        return self._adaptor.open_dir (tgt_url, flags, ttype=ttype)


    # ----------------------------------------------------------------
    #
    # replica methods
    #
    # --------------------------------------------------------------------------
    #
    @rus.takes   ('LogicalDirectory',
                  rus.one_of (ru.Url, str),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
    def get_size (self, tgt, ttype=None) :
        '''
        get_size(tgt)

        tgt:     logical file to get size for
        ttype:    saga.task.type enum
        ret:      int / saga.Task

        Returns the size of the physical file represented by the given logical
        file (in bytes)

           Example::

               # get a logical directory handle
               lf = saga.replica.LogicalFile("irods://localhost/tmp/data/")

               # print a logical file's size
               print(lf.get_size ('data.dat'))

        '''
        tgt_url = ru.Url (tgt)
        return self._adaptor.get_size (tgt_url, ttype=ttype)


    # ----------------------------------------------------------------
    #
    @rus.takes   ('LogicalDirectory',
                  rus.optional (str),
                  rus.optional (str),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (ru.Url), st.Task))
    def find (self, name_pattern, attr_pattern=None, flags=c.RECURSIVE,
              ttype=None) :
        '''
        find(name_pattern, attr_pattern=None, flags=RECURSIVE)

        name_pattern:   string
        attr_pattern:   string
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            list [saga.Url] / saga.Task

        '''
        if not flags: flags = 0

        if attr_pattern:
            return self._adaptor.find_replicas(name_pattern, attr_pattern,
                                               flags, ttype=ttype)
        else:
            return self._nsdirec.find(name_pattern, flags, ttype=ttype)


# ------------------------------------------------------------------------------

