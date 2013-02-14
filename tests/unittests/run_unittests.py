
import re
import os
import sys
import glob
import nose

import saga.utils.test_config as sutc

test_filter = None # by default run all tests
if len(sys.argv) > 1 :
    test_filter = sys.argv[1]
    del (sys.argv[1]) # otherwise nosetest will eval it :-/

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

        if  test_filter :
            if  not re.search (test_filter, test_suite) :
                print " skipping     %s : %s" % (os.path.basename (test_cfg), test_suite)
                continue

    
        # configure the test suite 
        config = nose.config.Config ()
        
        config.verbosity  = 1
        config.workingDir = test_base + '/' + test_suite
        config.stream     = sys.stderr
        
        # and run tests
        print
        print "======================================================================"
        print "testing with %s : %s" % (os.path.basename (test_cfg), test_suite)
        print "----------------------------------------------------------------------"
        result = nose.core.run (config=config)
        print "======================================================================"



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

