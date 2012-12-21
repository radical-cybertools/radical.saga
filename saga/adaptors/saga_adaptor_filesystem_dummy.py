
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

""" dummy filesystem adaptor implementation """

import os
import shutil

import saga.url
import saga.cpi.base
import saga.cpi.filesystem
import saga.utils.misc

from   saga.utils.singleton import Singleton

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async


_adaptor_name   = 'saga_adaptor_filesystem_dummyl'
_adaptor_info   = [{ 'name'    : _adaptor_name,
                     'type'    : 'saga.filesystem.Directory',
                     'class'   : 'DummyDirectory',
                     'schemas' : ['dummy']
                   }, 
                   { 'name'    : _adaptor_name,
                     'type'    : 'saga.filesystem.File',
                     'class'   : 'DummyFile',
                     'schemas' : ['dummy']
                   }]

###############################################################################
# adaptor info
#

_adaptor_name    = 'saga.adaptor.dummysystem.local'
_adaptor_options = []
_adaptor_schemas = ['dummy', 'local']
_adaptor_info    = {
    'name'          : _adaptor_name,
    'cpis'          : [{
        'type'    : 'saga.filesystem.Directory',
        'class'   : 'DummyDirectory',
        'schemas' : _adaptor_schemas
        }, 
        {
        'type'    : 'saga.filesystem.File',
        'class'   : 'DummyFile',
        'schemas' : _adaptor_schemas
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
    but engine is a singleton, too...) -- the engine will though create new CPI
    implementation instances as needed (one per SAGA API object).
    """

    __metaclass__ = Singleton


    def __init__ (self) :

        saga.cpi.base.AdaptorBase.__init__ (self, _adaptor_name, _adaptor_options)


    def register (self) :
        """ Adaptor registration function. The engine calls this during startup. 
    
            We usually do sanity checks here and throw and exception if we think
            the adaptor won't work in a given environment. In that case, the
            engine won't add it to it's internal list of adaptors. If everything
            is ok, we return the adaptor info.
        """
    
        return _adaptor_info


###############################################################################
#
class DummyDirectory (saga.cpi.filesystem.Directory) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'DummyDirectory')


    @SYNC
    # FIXME: where are the flags?
    def init_instance (self, url, flags, session) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()


    @ASYNC
    def init_instance_async (self, ttype, url, flags, session) :
        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()
        
        t = saga.task.Task ()

        t._set_result (saga.dummysystem.Directory._create_from_adaptor \
                       (url, flags, session, _adaptor_name))
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

            if saga.dummysystem.CREATE & flags :
                if saga.dummysystem.CREATE_PARENTS & flags :
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
        


    @SYNC
    def get_url (self) :

        return self._url


    @SYNC
    def open (self, url, flags) :
        
        if not url.scheme and not url.host : 
            url = saga.url.Url (str(self._url) + '/' + str(url))

        f = saga.dummysystem.File._create_from_adaptor (url, flags, self._session, 
                                                       _adaptor_name)
        return f


######################################################################
#
# file adaptor class
#
class DummyFile (saga.cpi.filesystem.File) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'DummyFile')


    @SYNC
    def init_instance (self, url, flags, session) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()


    @ASYNC
    def init_instance_async (self, ttype, url, flags, session) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()
        
        t = saga.task.Task ()

        t._set_result (saga.dummysystem.File._create_from_adaptor \
                       (url, flags, session, _adaptor_name))
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

    @SYNC
    def get_url (self) :
        return self._url

    @ASYNC
    def get_url_async (self, ttype) :

        t = saga.task.Task ()

        t._set_state  = saga.task.Done
        t._set_result = self._url

        return t


    @SYNC
    def get_size_self (self) :
        return os.path.getsize (self._url.path)


    @ASYNC
    def get_size_self_async (self, ttype) :

        t = saga.task.Task ()

        t._set_result (os.path.getsize (self._url.path))
        t._set_state  (saga.task.DONE)

        return t


    @SYNC
    def copy_self (self, target, flags) :

        tgt_url = saga.url.Url (target)
        tgt     = tgt_url.path
        src     = self._url.path

        if tgt_url.schema :
            if not tgt_url.schema in _adaptor_schemas :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (unknown schema)" %  target)

        if tgt[0] != '/' :
            tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print " copy %s %s" % (self._url, tgt)
        shutil.copy2 (src, tgt)

