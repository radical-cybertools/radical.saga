
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Local filesystem adaptor implementation """

import os
import pprint
import shutil
import traceback

import saga.url
import saga.adaptors.base
import saga.adaptors.cpi.filesystem

import saga.utils.misc

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'saga.adaptor.filesystem.local'
_ADAPTOR_SCHEMAS       = ['file', 'local']
_ADAPTOR_OPTIONS       = []
_ADAPTOR_CAPABILITIES  = {}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : """This adaptor interacts with local filesystem, by
                            using the (POSIX like) os and shutil Python packages.""",
    'schemas'          : {'file'  : 'local filesystem.', 
                          'local' : 'alias for *file*' 
    },
}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.2',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{
        'type'         : 'saga.filesystem.Directory',
        'class'        : 'LocalDirectory'
        }, 
        {
        'type'         : 'saga.filesystem.File',
        'class'        : 'LocalFile'
        }
    ]
}


###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.base.Base):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    def __init__ (self) :

        saga.adaptors.base.Base.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # the adaptor *singleton* creates a (single) instance of a bulk handler
        # (BulkDirectory), which implements container_* bulk methods.
        self._bulk = BulkDirectory ()


    def sanity_check (self) :
        # nothing to check for, local file system should always be accessible
        pass




###############################################################################
#
class BulkDirectory (saga.adaptors.cpi.filesystem.Directory) :
    """
    Well, this implementation can handle bulks, but cannot optimize them.
    We leave that code here anyway, for demonstration -- but those methods
    are also provided as fallback, and are thusly used if the adaptor does
    not implement the bulk container_* methods at all.
    """

    def __init__ (self) : 
        pass


    def container_wait (self, tasks, mode, timeout) :
      # print " ~ bulk wait ~~~~~~~~~~~~~~~~~~~~~ "
      # pprint.pprint (tasks)
        if timeout >= 0 :
            raise saga.exceptions.BadParameter ("Cannot handle timeouts > 0")
        for task in tasks :
            task.wait ()
      # print " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "


    def container_cancel (self, tasks) :
      # print " ~ bulk wait ~~~~~~~~~~~~~~~~~~~~~ "
      # pprint.pprint (tasks)
        for task in tasks :
            task.cancel ()
      # print " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "


    def container_copy (self, tasks) :
        """
        A *good* implementation would dig the file copy operations from the
        tasks, and run them in a bulk -- we can't do that, so simply *run* the
        individual tasks, falling back to the default non-bulk asynchronous copy
        operation...
        """
      # print " ~ bulk copy ~~~~~~~~~~~~~~~~~~~~~ "
      # pprint.pprint (tasks)
        for task in tasks :
            task.run ()
      # print " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "


    # the container methods for the other calls are obviously similar, and left
    # out here.  The :class:`saga.task.Container` implementation will fall back
    # to the non-bulk async calls for all then.



###############################################################################
#
class LocalDirectory (saga.adaptors.cpi.filesystem.Directory) :

    def __init__ (self, api, adaptor) :

        _cpi_base = super  (LocalDirectory, self)
        _cpi_base.__init__ (api, adaptor)


    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url       = url
        self._flags     = flags
        self._session   = session
        self._container = self._adaptor._bulk

        self._init_check ()

        return self.get_api ()


    @ASYNC_CALL
    def init_instance_async (self, adaptor_state, url, flags, session, ttype) :

        self._url     = url
        self._flags   = flags
        self._session = session
        
        c = { 'url'     : self._url, 
              'flags'   : self._flags,
              'session' : self._session }

        return saga.task.Task (self, 'init_instance', c, ttype)


    def _init_check (self) :

        url   = self._url
        flags = self._flags 

        if not saga.utils.misc.url_is_local (url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)"     %  url)
        if url.port :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has port)"      %  url)
        if url.fragment :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has fragment)"  %  url)
        if url.query :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has query)"     %  url)
        if url.username :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has username)"  %  url)
        if url.password :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has password)"  %  url)

        self._path = url.path
        path       = url.path

        if not os.path.exists (path) :

            if saga.filesystem.CREATE & flags :
                if saga.filesystem.CREATE_PARENTS & flags :
                    try :
                        os.makedirs (path)
                    except Exception as e :
                        raise saga.exceptions.NoSuccess ("Could not 'mkdir -p %s': %s)"  \
                                                        % (path, str(e)))
                else :
                    try :
                        os.mkdir (path)
                    except Exception as e :
                        raise saga.exceptions.NoSuccess ("Could not 'mkdir %s': %s)"  \
                                                        % (path, str(e)))
            else :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (directory does not exist)"  \
                                                   %  path)
        
        if not os.path.isdir (path) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (is not a directory)"  \
                                               %  path)
        


    @SYNC_CALL
    def get_url (self) :

        return self._url


    @SYNC_CALL
    def open (self, url, flags) :

      # print "sync open: - '%s' - '%s' - "  %  (url, flags)
        
        if not url.scheme and not url.host : 
            url = saga.url.Url (str(self._url) + '/' + str(url))

        return saga.filesystem.File (url, flags, self._session, _adaptor=self._adaptor)


    # # the async call implementation works, in pair with task_run.  As it is
    # disabled though, the threaded async fallback from the cpi layer will be
    # used.
    # @ASYNC_CALL
    # def open_async (self, url, flags, ttype) :
    #
    #     c = { 'url'     : url,
    #             'flags'   : flags}
    #
    #     return saga.task.Task (self, 'open', c, ttype)


    @SYNC_CALL
    def copy (self, source, target, flags) :

        src_url = saga.url.Url (source)
        src     = src_url.path
        tgt_url = saga.url.Url (target)
        tgt     = tgt_url.path


        if src_url.schema :
            if not src_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)" %  source)

        if not saga.utils.misc.url_is_local (src_url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)"     %  source)

        if src[0] != '/' :
            if not src_url.scheme and not src_url.host :
                if self._url.path[0] == '/' :
                    src = "%s/%s"   % (self._url.path, src)


        if tgt_url.schema :
            if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)" %  target)

        if not saga.utils.misc.url_is_local (tgt_url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)"     %  target)

        if tgt[0] != '/' :
            if not tgt_url.scheme and not tgt_url.host :
                if self._url.path[0] == '/' :
                    tgt = "%s/%s"   % (self._url.path, tgt)


        # FIXME: eval flags, check for existence, etc.


        if os.path.isdir (src) :
          # print "sync copy tree %s -> %s" % (src, tgt)
            shutil.copytree (src, tgt)

        else : 
          # print "sync copy %s -> %s" % (src, tgt)
            shutil.copy2 (src, tgt)



    @SYNC_CALL
    def copy_self (self, target, flags) :

        return self.copy (self._url, target, flags)


    @ASYNC_CALL
    def copy_async (self, src, tgt, flags, ttype) :

      # print "async copy %s -> %s [%s]" % (src, tgt, ttype)

        c = { 'src'     : src,
              'tgt'     : tgt,
              'flags'   : flags }

        return saga.task.Task (self, 'copy', c, ttype)



    def task_wait (self, task, timout) :
        # FIXME: our task_run moves all tasks into DONE state... :-/
        pass



    def task_run (self, task) :
        # FIXME: that should be generalized, possibly wrapped into a thread, and
        # moved to CPI level

        call = task._method_type
        c    = task._method_context

        if call == 'copy' :
            try :
                task._set_result (self.copy (c['src'], c['tgt'], c['flags']))
                task._set_state  (saga.task.DONE)
            except Exception as e :
                task._set_exception (e)
                task._set_state     (saga.task.FAILED)
        elif call == 'init_instance' :
            try :
                self.init_instance ({}, c['url'], c['flags'], c['session'])
                task._set_result (self.get_api ())
                task._set_state  (saga.task.DONE)
            except Exception as e :
                task._set_exception (e)
                task._set_state     (saga.task.FAILED)
        elif call == 'open' :
            try :
                task._set_result (self.open (c['url'], c['flags']))
                task._set_state  (saga.task.DONE)
            except Exception as e :
                task._set_exception (e)
                task._set_state     (saga.task.FAILED)
        else :
            raise saga.exceptions.NotImplemented ("Cannot handle %s tasks" %  call)




######################################################################
#
# file adaptor class
#
class LocalFile (saga.adaptors.cpi.filesystem.File) :

    def __init__ (self, api, adaptor) :

        _cpi_base = super  (LocalFile, self)
        _cpi_base.__init__ (api, adaptor)


    def _dump (self) :
        print "url    : %s"  % self._url
        print "flags  : %s"  % self._flags
        print "session: %s"  % self._session


    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()

        return self.get_api ()


    @ASYNC_CALL
    def init_instance_async (self, adaptor_state, url, flags, session, ttype) :

        self._url     = url
        self._flags   = flags
        self._session = session

        c = { 'url'     : self._url, 
              'flags'   : self._flags,
              'session' : self._session }
        
        t = saga.task.Task (self, 'init_instance', c, ttype)

        # FIXME: move to task_run...
        self._init_check ()

        t._set_result (saga.filesystem.File (url, flags, session, _adaptor=self._adaptor))
        t._set_state  (saga.task.DONE)

        return t


    def _init_check (self) :

        url   = self._url
        flags = self._flags 

        if not saga.utils.misc.url_is_local (url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)"     %  url)
        if url.port :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has port)"      %  url)
        if url.fragment :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has fragment)"  %  url)
        if url.query :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has query)"     %  url)
        if url.username :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has username)"  %  url)
        if url.password :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has password)"  %  url)

        self._path = url.path
        path       = url.path

        if not os.path.exists (path) :

            (dirname, filename) = os.path.split (path)

            if not filename :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (names directory)"  \
                                                 %  path)

            if not os.path.exists (dirname) :
                if saga.filesystem.CREATE_PARENTS & flags :
                    try :
                        os.makedirs (path)
                    except Exception as e :
                        raise saga.exceptions.NoSuccess ("Could not 'mkdir -p %s': %s)"  \
                                                        % (path, str(e)))
                else :
                    raise saga.exceptions.BadParameter ("Cannot handle url %s (parent dir does not exist)"  \
                                                     %  path)
        
            if not os.path.exists (filename) :
                if saga.filesystem.CREATE & flags :
                    try :
                        open (path, 'w').close () # touch
                    except Exception as e :
                        raise saga.exceptions.NoSuccess ("Could not 'touch %s': %s)"  \
                                                        % (path, str(e)))
                else :
                    raise saga.exceptions.BadParameter ("Cannot handle url %s (file does not exist)"  \
                                                     %  path)
        
        if not os.path.isfile (path) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (is not a file)"  \
                                               %  path)

    @SYNC_CALL
    def get_url (self) :
        return self._url

    @ASYNC_CALL
    def get_url_async (self, ttype) :

        c = {}
        t = saga.task.Task (self, 'get_url', c, ttype)

        # FIXME: move to task_run...
        t._set_state  (saga.task.DONE)
        t._set_result (self._url)

        return t


    @SYNC_CALL
    def get_size_self (self) :
        return os.path.getsize (self._url.path)


    @ASYNC_CALL
    def get_size_self_async (self, ttype) :

        c = {}
        t = saga.task.Task (self, 'get_size', c, ttype)

        # FIXME: move to task_run...
        t._set_result (os.path.getsize (self._url.path))
        t._set_state  (saga.task.DONE)

        return t


    @SYNC_CALL
    def copy_self (self, target, flags) :

        tgt_url = saga.url.Url (target)
        tgt     = tgt_url.path
        src     = self._url.path

        if tgt_url.schema :
            if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)" %  target)

        if not saga.utils.misc.url_is_local (tgt_url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)"     %  target)

        if tgt[0] != '/' :
            tgt = "%s/%s"   % (os.path.dirname (src), tgt)

      # print " copy %s %s" % (self._url, tgt)
        shutil.copy2 (src, tgt)



    def task_run (self, task) :
        # FIXME: that should be generalized, possibly wrapped into a thread, and
        # moved to CPI level

        call = task._method_type
        c    = task._method_context

        if call == 'copy_self' :
            try :
                task._set_result (self.copy_self (c['tgt'], c['flags']))
                task._set_state  (saga.task.DONE)
            except Exception as e :
                task._set_exception (e)
                task._set_state     (saga.task.FAILED)
        elif call == 'get_size' :
            try :
                task._set_result (self.get_size_self ())
                task._set_state  (saga.task.DONE)
            except Exception as e :
                task._set_exception (e)
                task._set_state     (saga.task.FAILED)
        else :
            raise saga.exceptions.NotImplemented ("Cannot handle %s tasks" %  call)




