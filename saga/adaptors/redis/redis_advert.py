
""" Redis advert adaptor implementation """

import pprint
import traceback

import saga.url
import saga.cpi.base
import saga.cpi.advert
import saga.utils.misc

from   saga.utils.singleton import Singleton

from   redis_namespace      import *

SYNC_CALL  = saga.cpi.base.SYNC_CALL
ASYNC_CALL = saga.cpi.base.ASYNC_CALL


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'saga.adaptor.advert.redis'
_ADAPTOR_SCHEMAS       = ['redis']
_ADAPTOR_OPTIONS       = []
_ADAPTOR_CAPABILITES   = {}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilites'      : _ADAPTOR_CAPABILITES,
    'description'      : 'The redis advert adaptor.',
    'details'          : """This adaptor interacts with a redis server to
                            implement the advert API semantics.""", 
    'schemas'          : {'redis'  : 'redis nosql backend.'}
}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.2',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{
        'type'         : 'saga.advert.Directory',
        'class'        : 'RedisDirectory'
        }, 
        {
        'type'         : 'saga.advert.Entry',
        'class'        : 'RedisEntry'
        }
    ]
}


###############################################################################
# The adaptor class

class Adaptor (saga.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.

    We only need one instance of this adaptor per process (actually per engine,
    but engine is a singleton, too...) -- the engine will create new adaptor
    class instances (see below) as needed (one per SAGA API object).
    """

    __metaclass__ = Singleton


    def __init__ (self) :

        saga.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # the adaptor *singleton* creates a (single) instance of a bulk handler
        # (BulkDirectory), which implements container_* bulk methods.
        self._bulk  = BulkDirectory ()
        self._redis = {}


    def get_redis (self, url) :

        host     = None
        port     = 6379
        username = None
        password = None
     
        if url.host     : host     = url.host
        if url.port     : port     = url.port
        if url.username : username = url.username
        if url.password : password = url.password

        if username :
            if password :
                hash = "redis://%s:%s@%s:%d"  %  (username, password, host, port)
            else :
                hash = "redis://%s@%s:%d"     %  (username,           host, port)
        else :
            if password :
                hash = "redis://%s@%s:%d"     %  (password,       host, port)
            else :
                hash = "redis://%s:%d"        %  (                host, port)
       
        if not hash in self._redis :
            print "init redis for %s" %  hash
            self._redis[hash] = redis_ns_init (url)

        return self._redis[hash]


    def sanity_check (self) :
        # nothing to check for, redis entry system should always be accessible
        pass




###############################################################################
#
class BulkDirectory (saga.cpi.advert.Directory) :
    """
    Well, this implementation can handle bulks, but cannot optimize them.
    We leave that code here anyway, for demonstration -- but those methods
    are also provided as fallback, and are thusly used if the adaptor does
    not implement the bulk container_* methods at all.
    """

    def __init__ (self) : 
        pass


    def container_wait (self, tasks, mode, timeout) :
        print " ~ bulk wait ~~~~~~~~~~~~~~~~~~~~~ "
        pprint.pprint (tasks)
        if timeout >= 0 :
            raise saga.exceptions.BadParameter ("Cannot handle timeouts > 0")
        for task in tasks :
            task.wait ()
        print " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "


    def container_cancel (self, tasks) :
        print " ~ bulk wait ~~~~~~~~~~~~~~~~~~~~~ "
        pprint.pprint (tasks)
        for task in tasks :
            task.cancel ()
        print " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "


    def container_copy (self, tasks) :
        """
        A *good* implementation would dig the entry copy operations from the
        tasks, and run them in a bulk -- we can't do that, so simply *run* the
        individual tasks, falling back to the default non-bulk asynchronous copy
        operation...
        """
        print " ~ bulk copy ~~~~~~~~~~~~~~~~~~~~~ "
        pprint.pprint (tasks)
        for task in tasks :
            task.run ()
        print " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "


    # the container methods for the other calls are obviously similar, and left
    # out here.  The :class:`saga.task.Container` implementation will fall back
    # to the non-bulk async calls for all then.



###############################################################################
#
class RedisDirectory (saga.cpi.advert.Directory, saga.cpi.Async) :

    def __init__ (self, api, adaptor) :

        saga.cpi.CPIBase.__init__ (self, api, adaptor)


    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url       = url
        self._flags     = flags
        self._session   = session
        self._container = self._adaptor._bulk

        self._init_check ()

        return self._api


    @SYNC_CALL
    def attribute_getter (self, key) :
        if key == '_adaptor' :
            raise saga.exceptions.NotImplemented ('yet')
        e = redis_ns_get (self._r, self._url.path)
        return e.data[key]


    @SYNC_CALL
    def attribute_setter (self, key, val) :

        #  FIXME: we need a better way to ignore local attributes.  This needs
        #  fixing in the attribute interface implementation...
        if key == '_adaptor' :
            raise saga.exceptions.NotSuccess ('self._adaptor is private to the api class')

        redis_ns_datakey_set (self._r, self._url.path, key, val)


    @SYNC_CALL
    def attribute_lister (self) :
        return ['helllooooouuuu']


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

        self._r     = self._adaptor.get_redis (self._url)
        self._nsdir = redis_ns_opendir (self._r, self._url.path, self._flags)



    @SYNC_CALL
    def get_url (self) :

        return self._url


    @SYNC_CALL
    def open (self, url, flags) :

        print "sync open: - '%s' - '%s' - "  %  (url, flags)
        
        if not url.scheme and not url.host : 
            url = saga.url.Url (str(self._url) + '/' + str(url))

        return saga.advert.Entry (url, flags, self._session, _adaptor=self._adaptor)


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
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)" %  source)

        if tgt_url.schema :
            if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)" %  target)


        # make paths absolute
        if src[0] != '/'  :  src = "%s/%s"   % (os.path.dirname (src), src) 
        if tgt[0] != '/'  :  tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print "sync copy %s -> %s" % (src, tgt)
        shutil.copy2 (src, tgt)


    @ASYNC_CALL
    def copy_async (self, src, tgt, flags, ttype) :

        print "async copy %s -> %s [%s]" % (src, tgt, ttype)

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
                task._set_result (self._api)
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
# entry adaptor class
#
class RedisEntry (saga.cpi.advert.Entry) :

    def __init__ (self, api, adaptor) :

        saga.cpi.CPIBase.__init__ (self, api, adaptor)



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

        return self


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

        t._set_result (saga.advert.Entry (url, flags, session, _adaptor=self._adaptor))
        t._set_state  (saga.task.DONE)

        return t


    def _init_check (self) :

        self._r       = self._adaptor.get_redis (self._url)
        self._nsentry = redis_ns_open (self._r, self._url.path, self._flags)


    @SYNC_CALL
    def attribute_getter (self, key) :
        if key == '_adaptor' :
            raise saga.exceptions.NotImplemented ('yet')
        e = redis_ns_get (self._r, self._url.path)
        return e.data[key]


    @SYNC_CALL
    def attribute_setter (self, key, val) :

        #  FIXME: we need a better way to ignore local attributes.  This needs
        #  fixing in the attribute interface implementation...
        if key == '_adaptor' :
            raise saga.exceptions.NotSuccess ('self._adaptor is private to the api class')

        redis_ns_datakey_set (self._r, self._url.path, key, val)


    @SYNC_CALL
    def attribute_lister (self) :
        return ['helllooooouuuu']


    @SYNC_CALL
    def get_url (self) :
        return self._url


    @ASYNC_CALL
    def get_url_async (self, ttype) :

        c = {}
        t = saga.task.Task (self, 'get_url', c, ttype)

        # FIXME: move to task_run...
        t._set_state  = saga.task.Done
        t._set_result = self._url

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
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)" %  target)

        if not saga.utils.misc.url_is_redis (tgt_url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)"     %  target)

        # make path absolute
        if tgt[0] != '/'  :  tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print " copy %s %s" % (self._url, tgt)
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
                task._set_result (self.get_size ())
                task._set_state  (saga.task.DONE)
            except Exception as e :
                task._set_exception (e)
                task._set_state     (saga.task.FAILED)
        else :
            raise saga.exceptions.NotImplemented ("Cannot handle %s tasks" %  call)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

