
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

""" iRODS replica adaptor implementation """

import saga.url
import saga.cpi.base
import saga.cpi.replica
import saga.utils.misc

from   saga.utils.singleton import Singleton

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'saga.adaptor.replica.irods'
_ADAPTOR_OPTIONS       = {}
_ADAPTOR_CAPABILITES   = {}
_ADAPTOR_SCHEMAS       = ['irods']

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilites'      : _ADAPTOR_CAPABILITES,
    'description'      : 'The iRODS replica adaptor.',
    'details'          : """This adaptor interacts with the irids data
                            management system, by using the irods command line
                            tools.""",
    'schemas'          : {'irods'  : 'irods schema'
    },
}

_ADAPTOR_REGISTRY      = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'cpis'             : [{
        'type'         : 'saga.replica.LogicalDirectory',
        'class'        : 'IRODSDirectory',
        'schemas'      : _ADAPTOR_SCHEMAS
        }, 
        {
        'type'         : 'saga.replica.LogicalFile',
        'class'        : 'IRODSFile',
        'schemas'      : _ADAPTOR_SCHEMAS
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

        saga.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_NAME, _ADAPTOR_OPTIONS)


    def register (self) :
        """ Adaptor registration function. The engine calls this during startup. 
    
            We usually do sanity checks here and throw and exception if we think
            the adaptor won't work in a given environment. In that case, the
            engine won't add it to it's internal list of adaptors. If everything
            is ok, we return the adaptor info.
        """
    
        return _ADAPTOR_REGISTRY


###############################################################################
#
class IRODSDirectory (saga.cpi.replica.LogicalDirectory) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'IRODSDirectory')


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

        t._set_result (saga.replica.LogicalDirectory._create_from_adaptor \
                       (url, flags, session, _ADAPTOR_NAME))
        t._set_state  (saga.task.DONE)

        return t


    def _init_check (self) :

        url   = self._url
        flags = self._flags 

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

            if saga.replica.CREATE & flags :
                if saga.replica.CREATE_PARENTS & flags :
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
                raise saga.exceptions.BadParameter ("Cannot handle url %s (Logicaldirectory does not exist)"  \
                                                   %  path)
        
        if not os.path.isdir (path) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (is not a Logicaldirectory)"  \
                                               %  path)
        


    @SYNC
    def get_url (self) :

        return self._url


    @SYNC
    def open (self, url, flags) :
        
        if not url.scheme and not url.host : 
            url = saga.url.Url (str(self._url) + '/' + str(url))

        f = saga.replica.LogicalFile._create_from_adaptor (url, flags, self._session, _ADAPTOR_NAME)
        return f


######################################################################
#
# Logicalfile adaptor class
#
class IRODSFile (saga.cpi.replica.LogicalFile) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'IRODSFile')


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

        t._set_result (saga.replica.LogicalFile._create_from_adaptor \
                       (url, flags, session, _ADAPTOR_NAME))
        t._set_state  (saga.task.DONE)

        return t


    def _init_check (self) :

        url   = self._url
        flags = self._flags 

        if url.port :
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
                if saga.replica.CREATE_PARENTS & flags :
                    try :
                        os.makedirs (path)
                    except Exception as e :
                        raise saga.exceptions.NoSuccess ("Could not 'mkdir -p %s': %s)"  \
                                                        % (path, str(e)))
                else :
                    raise saga.exceptions.BadParameter ("Cannot handle url %s (parent dir does not exist)"  \
                                                     %  path)
        
            if not os.path.exists (filename) :
                if saga.replica.CREATE & flags :
                    try :
                        open (path, 'w').close () # touch
                    except Exception as e :
                        raise saga.exceptions.NoSuccess ("Could not 'touch %s': %s)"  \
                                                        % (path, str(e)))
                else :
                    raise saga.exceptions.BadParameter ("Cannot handle url %s (Logicalfile does not exist)"  \
                                                     %  path)
        
        if not os.path.isfile (path) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (is not a Logicalfile)"  \
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
            if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url schema for %s" %  target)

        if tgt[0] != '/' :
            tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print " copy %s %s" % (self._url, tgt)
        shutil.copy2 (src, tgt)

