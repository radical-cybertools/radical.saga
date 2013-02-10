
""" Redis advert adaptor implementation """

import traceback

import saga.url
import saga.adaptors.cpi.base
import saga.adaptors.cpi.advert
import saga.utils.misc

from   redis_namespace      import *

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'saga.adaptor.advert.redis'
_ADAPTOR_SCHEMAS       = ['redis']
_ADAPTOR_OPTIONS       = []
_ADAPTOR_CAPABILITIES  = {}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
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

class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # the adaptor *singleton* creates a (single) instance of a bulk handler
        # (BulkDirectory), which implements container_* bulk methods.
        self._bulk  = BulkDirectory ()
        self._redis = {}


    # ----------------------------------------------------------------
    #
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
            self._redis[hash] = redis_ns_server (url)

        return self._redis[hash]


    # ----------------------------------------------------------------
    #
    def sanity_check (self) :
        # nothing to check for, redis entry system should always be accessible
        pass




###############################################################################
#
class BulkDirectory (saga.adaptors.cpi.advert.Directory) :
    """
    Well, this implementation can handle bulks, but cannot optimize them.
    We leave that code here anyway, for demonstration -- but those methods
    are also provided as fallback, and are thusly used if the adaptor does
    not implement the bulk container_* methods at all.
    """

    # ----------------------------------------------------------------
    #
    def __init__ (self) : 
        pass


    # ----------------------------------------------------------------
    #
    def container_wait (self, tasks, mode, timeout) :
        if timeout >= 0 :
            raise saga.exceptions.BadParameter ("Cannot handle timeouts > 0")
        for task in tasks :
            task.wait ()


    # ----------------------------------------------------------------
    #
    def container_cancel (self, tasks) :
        for task in tasks :
            task.cancel ()


    # ----------------------------------------------------------------
    #
    def container_copy (self, tasks) :
        """
        A *good* implementation would dig the entry copy operations from the
        tasks, and run them in a bulk -- we can't do that, so simply *run* the
        individual tasks, falling back to the default non-bulk asynchronous copy
        operation...
        """
        for task in tasks :
            task.run ()


    # the container methods for the other calls are obviously similar, and left
    # out here.  The :class:`saga.task.Container` implementation will fall back
    # to the non-bulk async calls for all then.



###############################################################################
#
class RedisDirectory (saga.adaptors.cpi.advert.Directory) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (RedisDirectory, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url       = url
        self._flags     = flags
        self._container = self._adaptor._bulk

        self._set_session (session)
        self._init_check  ()

        return self.get_api ()


    # ----------------------------------------------------------------
    #
    @ASYNC_CALL
    def init_instance_async (self, adaptor_state, url, flags, session, ttype) :

        self._url     = url
        self._flags   = flags
        
        self._set_session (session)
        
        c = { 'url'     : self._url, 
              'flags'   : self._flags }

        return saga.task.Task (self, 'init_instance', c, ttype)


    # ----------------------------------------------------------------
    #
    def _init_check (self) :

        self._r     = self._adaptor.get_redis (self._url)
        self._nsdir = redis_ns_entry.opendir (self._r, self._url.path, self._flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_getter (self, key) :

        try :
            return self._nsdir.get_key (key)

        except Exception as e :
            self._logger.error ("get_key failed: %s" % e)
            raise e


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_setter (self, key, val) :

        try :
            self._nsdir.set_key (key, val)

        except Exception as e :
            self._logger.error ("set_key failed: %s" % e)
            raise e


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_lister (self) :
        return self._nsdir.get_data ().keys ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_caller (self, key, id, cb) :
        self._nsdir.manage_callback (key, id, cb, self.get_api ())


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url (self) :
        return self._url


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open (self, url, flags) :

        if not url.scheme and not url.host : 
            url = saga.url.Url (str(self._url) + '/' + str(url))

        return saga.advert.Entry (url, flags, self._session, _adaptor=self._adaptor)


  # ##################################################################
  # # FIXME: all below
  # @SYNC_CALL
  # def copy (self, source, target, flags) :
  #     return
  # 
  #     src_url = saga.url.Url (source)
  #     src     = src_url.path
  #     tgt_url = saga.url.Url (target)
  #     tgt     = tgt_url.path
  # 
  # 
  #     if src_url.schema :
  #         if not src_url.schema.lower () in _ADAPTOR_SCHEMAS :
  #             raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)" %  source)
  # 
  #     if tgt_url.schema :
  #         if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
  #             raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)" %  target)
  # 
  # 
  #     # make paths absolute
  #     if src[0] != '/'  :  src = "%s/%s"   % (os.path.dirname (src), src) 
  #     if tgt[0] != '/'  :  tgt = "%s/%s"   % (os.path.dirname (src), tgt)
  # 
  #     shutil.copy2 (src, tgt)
  # 
  # 
  # @ASYNC_CALL
  # def copy_async (self, src, tgt, flags, ttype) :
  # 
  #     c = { 'src'     : src,
  #           'tgt'     : tgt,
  #           'flags'   : flags }
  # 
  #     return saga.task.Task (self, 'copy', c, ttype)
  # 
  # 
  # 
  # def task_wait (self, task, timout) :
  #     # FIXME: our task_run moves all tasks into DONE state... :-/
  #     pass



######################################################################
#
# entry adaptor class
#
class RedisEntry (saga.adaptors.cpi.advert.Entry) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (RedisEntry, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    def _dump (self) :
        self._logger.debug ("url  : %s"  % self._url)
        self._logger.debug ("flags: %s"  % self._flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url     = url
        self._flags   = flags

        self._set_session (session)
        self._init_check  ()

        return self


    # ----------------------------------------------------------------
    #
    def _init_check (self) :

        self._r       = self._adaptor.get_redis (self._url)
        self._nsentry = redis_ns_entry.open (self._r, self._url.path, self._flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_getter (self, key) :

        return self._nsentry.get_key (key)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_setter (self, key, val) :

        return self._nsentry.set_key (key, val)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_lister (self) :

        return self._nsentry.get_data ().keys ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def attribute_caller (self, key, id, cb) :

        return self._nsentry.manage_callback (key, id, cb, self.get_api ())


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url (self) :

        return self._url


  # ##################################################################
  # # FIXME: all below
  # @SYNC_CALL
  # def copy_self (self, target, flags) :
  # 
  #     tgt_url = saga.url.Url (target)
  #     tgt     = tgt_url.path
  #     src     = self._url.path
  # 
  #     if tgt_url.schema :
  #         if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
  #             raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)" %  target)
  # 
  #     if not saga.utils.misc.url_is_redis (tgt_url) :
  #         raise saga.exceptions.BadParameter ("Cannot handle url %s (not redis)"     %  target)
  # 
  #     # make path absolute
  #     if tgt[0] != '/'  :  tgt = "%s/%s"   % (os.path.dirname (src), tgt)
  # 
  #     shutil.copy2 (src, tgt)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

