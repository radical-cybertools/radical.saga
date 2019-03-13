
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import pytest
import radical.saga as rs

import radical.utils as ru

# ------------------------------------------------------------------------------
#
def pytest_addoption (parser) :
    parser.addoption ("--configs", action="append", default=[],
        help="list of configs to use for testing")

# ------------------------------------------------------------------------------
#
def pytest_generate_tests (metafunc) :
    if 'configs' in metafunc.fixturenames :
        metafunc.parametrize ("configs",
                              metafunc.config.option.configs,
                              scope="module")

# ------------------------------------------------------------------------------
#
@pytest.fixture (scope="module")
def cfg (configs) :

    fname = "./configs/%s" % configs

    if  not os.path.exists (fname) :
        raise Exception ("no such config: '%s'" % fname)

    cfg = ru.read_json_str (fname)

    assert    ('rs.tests' in cfg)
    return cfg['rs.tests']



# ------------------------------------------------------------------------------
#
@pytest.fixture (scope="module")
def session (cfg) :

    s = saga.Session ()
    t = cfg.get ('context_tye')

    if  t :
        c = saga.Context (t)

        c.context_user_proxy = cfg.get ('context_user_proxy')
        c.context_user_id    = cfg.get ('context_user_id')
        c.context_user_pass  = cfg.get ('context_user_pass')

        s.add_context (c)

    return s

# ------------------------------------------------------------------------------
#
@pytest.fixture (scope="module")
def tools (cfg) :

    # --------------------------------------------------------------------------
    #
    class Tools (object) :

        # ----------------------------------------------------------------------
        #
        @staticmethod
        def configure_jd (cfg, jd) :
        
            for a in ['job_walltime_limit' ,
                      'job_project'        ,
                      'job_queue'          ,
                      'job_total_cpu_count',
                      'job_spmd_variation' ] :
        
                if  cfg.get (a, None) :
                    jd.set_attribute (a, cfg[a])


        # ----------------------------------------------------------------------
        #
        @staticmethod
        def assert_exception (cfg, e) :
        
            ni = cfg.get ('not_implemented', 'warn')
        
            if  'NotImplemented' in str(e) and ni == warn :
                print "WARNING: %s"
                return
        
            else :
                assert (False), "unexpected exception '%s'" % e
                raise e
        
        
        # ----------------------------------------------------------------------
        #
        @staticmethod
        def silent_cancel (obj) :
        
            if  not isinstance (obj, list) :
                obj = [obj]
            
            for o in obj :
                try :
                    o.cancel ()
                except Exception :
                    pass
        
        
        # ----------------------------------------------------------------------
        #
        @staticmethod
        def silent_close (obj) :
        
            if  not isinstance (obj, list) :
                obj = [obj]
            
            for o in obj :
                try :
                    o.close ()
                except Exception :
                    pass

    # --------------------------------------------------------------------------
    #
    return Tools ()
    

# ------------------------------------------------------------------------------

