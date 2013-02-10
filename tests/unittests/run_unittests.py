
import os
import sys
import glob
import nose

import saga.utils.test_config as sutc

# this assumes that test suite is running from the root source directory.
pwd       = os.getcwd ()
test_base = pwd + "/tests/unittests/" 
test_cfgs = glob.glob (test_base + "/test_*.cfg")

for test_cfg in test_cfgs :

    # initialize the correct test_util singleton (i.e. with the correct configfile)
    tc = sutc.TestConfig ()
    tc.read_config (test_cfg)

    test_suites = tc.test_suites

    for test_suite in test_suites :
    
        # configure the test suite 
        config = nose.config.Config ()
        
        config.verbosity  = 0
        config.workingDir = test_base + '/' + test_suite
        
        # and run tests
        result = nose.core.run (config=config)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

