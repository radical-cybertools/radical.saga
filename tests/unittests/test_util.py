
import saga.exceptions      as se
import saga.utils.singleton as single
import saga.utils.logger    as slog
import saga.utils.config    as sconf

import saga

############# These are all supported options for saga.engine ####################
##
_config_options = [
    { 
    'category'      : 'saga.tests',
    'name'          : 'job_service_url', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "job submission url to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_JOB_SERVICE_URL"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'filesystem_url', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "filesystem root directory to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_FILESYSTEM_URL"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'replica_url', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "replica root directory to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_REPLICA_URL"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'advert_url', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "advert root directory to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_ADVERT_URL"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'context_type', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "context type to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_CONTEXT_TYPE"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'context_user_id', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "context user id to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_CONTEXT_USER_ID"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'context_user_pass', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "context user password to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_CONTEXT_USER_PASS"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'context_user_proxy', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "context user proxy to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_CONTEXT_USER_PROXY"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'context_user_cert', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "context user cert to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_CONTEXT_USER_CERT"
    },
]

################################################################################
##
class TestUtil (sconf.Configurable): 

    __metaclass__ = single.Singleton


    #-----------------------------------------------------------------
    # 
    def __init__(self):
        
        # set the configuration options for this object
        sconf.Configurable.__init__(self, 'saga.tests', _config_options)
        self._cfg = self.get_config()

        # Initialize the logging
        self._logger = slog.getLogger ('saga.tests')


    #-----------------------------------------------------------------
    # 
    def get_context (self):
        
        cfg   = self._cfg
        ctype = cfg['context_type'].get_value ()

        if not ctype :
            return None

        c = saga.Context (ctype)

        c.user_id    = cfg['context_user_id'].get_value ()
        c.user_pass  = cfg['context_user_pass'].get_value ()
        c.user_cert  = cfg['context_user_cert'].get_value ()
        c.user_proxy = cfg['context_user_proxy'].get_value ()

        return c

    #-----------------------------------------------------------------
    # 
    def get_session (self):
        
        # don't populate session with default contexts...
        s = saga.Session (default=False) 
        c = self.get_context ()

        if c :
            s.add_context (c)

        return s

    #-----------------------------------------------------------------
    # 
    def get_js_url (self):

        return self._cfg['job_service_url'].get_value ()


    #-----------------------------------------------------------------
    # 
    def get_fs_url (self):

        return self._cfg['job_filesystem_url'].get_value ()


    #-----------------------------------------------------------------
    # 
    def get_replica_url (self):

        return self._cfg['job_replica_url'].get_value ()


    #-----------------------------------------------------------------
    # 
    def get_advert_url (self):

        return self._cfg['job_advert_url'].get_value ()



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

