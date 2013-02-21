
import re
import os
import sys
import glob
import nose

import saga.utils.test_config as sutc

"""
This script runs a set of unit tests, which are organized in a directory
structure under ``tests/unittests/`` -- each sub-directory in that hierarchy
represents a test suite to be run.   

The test suites expects the python environment to be set up in a way that the
sage module is automatically found.  Also, it needs the ``nose`` module
installed (``easy_install nose`` should do the trick), which provides the
nosetests testing framework.

A set of config files (test_*.cfg) in ``tests/unittests/`` is used to configure
how the individual test suites are run.  The script accepts string parameters
which are interpreted as filters to limit the set of accepted test config files.
For example, the argument *job* would enable the test configuration
*test_local_job.cfg*, but not *test_local_file.cfg*.

The config files are in particular used to accomodate remote unit testing, i.e.
to run the unit tests against arbitrary remote backends.  An example config file
is::

    # this config file will run the job package unit tests against 
    # over a local ssh connection
    
    [saga.tests]
    test_suites        = engine,api/job

    job_service_url    = ssh://localhost/
    filesystem_url     = 
    replica_url        = 
    advert_url         = 

    context_type       = ssh
    context_user_id    = peer_gynt
    context_user_pass  = 
    context_user_proxy = 
    context_user_cert  = 

The above listing includes the complete set of supported attributes -- missing
entries are assumed to be empty strings, and can be left out.  The
``test_suites`` attribute MUST be set to include a list of test suites to run
against the given configuration.

The unit tests in the individual test suites will have access to the same
configuration info, and will use the given URL and context parameters to set up
the test environment.  For example, the api/job/test_service.py unit test will
use the following::

    import saga.utils.test_config as sutc
    ...

    tc = sutc.TestConfig ()
    js = saga.job.Service (tc.js_url, tc.session)

The :class:`saga.utils.test_config.TestConfig` class will expose the currently
active test configuration -- which is activated in the *run_unittests* script
as follows::

    tc = sutc.TestConfig ()
    tc.read_config (sfg_name)

Since :class:`saga.utils.test_config.TestConfig` is
a :class:`saga.utils.singleton.Singleton`, the thusly set state will be shared
with the test suites as shown.

"""

# by default run all tests
cfg_filters = [".*"] 

if len(sys.argv) > 1 :
    # if any arguments are specified, interpret them as filters on the config
    # file names.
    cfg_filters = list(sys.argv)

    # we need to purge the now interpreted arguments -- otherwise nosetest will
    # try to make sense of them...
    del (sys.argv[1:])


# we assume that test suite is running from the root source directory.
test_base = os.getcwd () + "/tests/unittests/" 
test_cfgs = glob.glob (test_base + "/test_*.cfg")

# is that assumption valid?
if  not test_cfgs :
    print "Could not find any config files -- was %s run from the source root?" % sys.argv[0]
    sys.exit (-1)


# the TestConfig singleton is shared with all test suites as they run
tc = sutc.TestConfig ()

# now cycle over the found test configs, configure the TestConfig accordingly,
# and run all specified test_suites
for sfg_name in test_cfgs :

    # only use this config if it matches the config filters.
    cfg_active = False
    for cfg_filter in cfg_filters :
        if  re.search (cfg_filter, sfg_name) :
            cfg_active = True
            break

    if  not cfg_active :
        # this config file name did not match *any* of the given filters...
        print "skipping %s" % (os.path.basename (sfg_name))
        continue

    
    # initialize the correct test_util singleton (i.e. with the correct configfile)
    tc.read_config (sfg_name)

    # run all test suites from the config
    for test_suite in tc.test_suites :

        # configure the unit test framework
        config = nose.config.Config ()
        
        config.verbosity  = 1
        config.workingDir = test_base + '/' + test_suite
        config.stream     = sys.stderr
        
        # and run tests
        print "______________________________________________________________________"
        print "%s : %s" % (os.path.basename (sfg_name), test_suite)
        result = nose.core.run (config=config)
        print "______________________________________________________________________"

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

