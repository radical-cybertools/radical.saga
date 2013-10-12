
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import copy

import radical.utils          as ru
import radical.utils.logger   as rul

import saga.exceptions        as se

import saga

# ------------------------------------------------------------------------------
#
def add_tc_params_to_jd (tc, jd):

    if tc['job_walltime_limit']  : jd.wall_time_limit = tc['job_walltime_limit'] 
    if tc['job_project']         : jd.project         = tc['job_project']        
    if tc['job_queue']           : jd.queue           = tc['job_queue']          
    if tc['job_total_cpu_count'] : jd.total_cpu_count = tc['job_total_cpu_count']
    if tc['job_spmd_variation']  : jd.spmd_variation  = tc['job_spmd_variation'] 

    return jd


# ------------------------------------------------------------------------------
#
class TestConfig (ru.TestConfig): 

    #-----------------------------------------------------------------
    # 
    def __init__ (self, cfg_file):

        # initialize configuration
        ru.TestConfig.__init__ (self, cfg_file)

        # Initialize the logging
        self._logger = rul.getLogger ('saga', 'tests')


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

