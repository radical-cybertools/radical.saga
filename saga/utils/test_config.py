
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import copy

import saga.exceptions      as se
import saga.utils.singleton as single
import saga.utils.logger    as slog
import saga.utils.config    as sconf

import saga

# ------------------------------------------------------------------------------
#
def add_tc_params_to_jd(tc, jd):

    if tc.job_walltime_limit  : jd.wall_time_limit = tc.job_walltime_limit
    if tc.job_project         : jd.project         = tc.job_project
    if tc.job_queue           : jd.queue           = tc.job_queue
    if tc.job_total_cpu_count : jd.total_cpu_count = tc.job_total_cpu_count
    if tc.job_spmd_variation  : jd.spmd_variation  = tc.job_spmd_variation

    return jd


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
    'name'          : 'job_project', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "project / allocation id to use with job scheduler",
    'env_variable'  : "SAGA_TEST_JOB_PROJECT"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'job_queue', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "queue to use with job scheduler",
    'env_variable'  : "SAGA_TEST_JOB_QUEUE"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'job_walltime_limit', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "walltime limit to pass to the job scheduler",
    'env_variable'  : "SAGA_TEST_JOB_WALLTIME_LIMIT"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'job_total_cpu_count', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "number of processes to pass to the job scheduler",
    'env_variable'  : "SAGA_TEST_NUMBER_OF_PROCESSES"
    },
    { 
    'category'      : 'saga.tests',
    'name'          : 'job_spmd_variation', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "spmd variation to pass to the job scheduler",
    'env_variable'  : "SAGA_TEST_SPMD_VARIATION"
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
    'name'          : 'replica_resource', 
    'type'          : str, 
    'default'       : "",
    'documentation' : "replica resource to be used for remote unit tests",
    'env_variable'  : "SAGA_TEST_REPLICA_RESOURCE"
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
    { 
    'category'      : 'saga.tests',
    'name'          : 'test_suites', 
    'type'          : list, 
    'default'       : ['utils', 'engine', 'api'],
    'documentation' : "a list of test suites to run (subdirs in tests/unittests/)",
    'env_variable'  : "SAGA_TEST_SUITES"
    },
]

################################################################################
##
class TestConfig (sconf.Configurable): 

    __metaclass__ = single.Singleton


    #-----------------------------------------------------------------
    # 
    def __init__ (self):

        # set the default configuration options for this object
        sconf.Configurable.__init__(self, 'saga.tests', _config_options)

        self._global_cfg = sconf.Configuration ()

        self.read_config ()

        # Initialize the logging
        self._logger = slog.getLogger ('saga.tests')

        self._test_cfg_d  = {}
        self._bench_cfg_d = {}


    #-----------------------------------------------------------------
    # 
    def read_config (self, configfile=None) :

        # re-initialize config for the given configfile
        if configfile :
            self._global_cfg._initialize (add_cfg_file=configfile)

        # need to make a deep copy here -- otherwise later tests which
        # re-trigger the read of the saga configuration will mess with our
        # config...
        self._tcfg = copy.deepcopy (self._global_cfg.get_category ('saga.tests'))

        if  self._global_cfg.has_category ('saga.benchmark') :
            self._bench_cfg_d = self._global_cfg.as_dict ('saga.benchmark')

        if  self._global_cfg.has_category ('saga.tests') :
            self._test_cfg_d = self._global_cfg.as_dict ('saga.tests')


    #-----------------------------------------------------------------
    # 
    @property
    def test_suites (self):
        
        return self._tcfg['test_suites'].get_value ()


    #-----------------------------------------------------------------
    # 
    def get_test_config      (self): return self._test_cfg_d
    def get_benchmark_config (self): return self._bench_cfg_d


    #-----------------------------------------------------------------
    # 
    @property
    def context (self):
        
        cfg          = self._tcfg
        c_type       = cfg['context_type'].get_value ()
        c_user_id    = cfg['context_user_id'].get_value ()
        c_user_pass  = cfg['context_user_pass'].get_value ()
        c_user_cert  = cfg['context_user_cert'].get_value ()
        c_user_proxy = cfg['context_user_proxy'].get_value ()

        if  not c_type :
            if  c_user_id    or c_user_pass  or \
                c_user_cert  or c_user_proxy :
                self._logger.warn ("ignoring incomplete context")
            return None

        c = saga.Context (c_type)

        c.user_id    = c_user_id
        c.user_pass  = c_user_pass
        c.user_cert  = c_user_cert
        c.user_proxy = c_user_proxy


        return c

    #-----------------------------------------------------------------
    # 
    @property
    def session (self):

        # don't populate session with default contexts...
        s = saga.Session (default=False) 
        c = self.context

        if c :
            s.add_context (c)
            try :
                pass
            except saga.exceptions.BadParameter as e :
                print "ERROR: could not use invalid context"

        return s

    #-----------------------------------------------------------------
    # 
    @property
    def js_url (self):

        return self._tcfg['job_service_url'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def job_walltime_limit (self):

        return self._tcfg['job_walltime_limit'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def job_project (self):

        return self._tcfg['job_project'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def job_queue (self):

        return self._tcfg['job_queue'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def job_total_cpu_count (self):

        return self._tcfg['job_total_cpu_count'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def job_spmd_variation (self):

        return self._tcfg['job_spmd_variation'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def filesystem_url (self):

        return self._tcfg['filesystem_url'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def replica_url (self):

        return self._tcfg['job_replica_url'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def replica_resource (self):

        return self._tcfg['job_replica_resource'].get_value ()


    #-----------------------------------------------------------------
    # 
    @property
    def advert_url (self):

        return self._tcfg['advert_url'].get_value ()

