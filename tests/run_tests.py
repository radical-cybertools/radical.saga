
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import re
import os
import sys
import glob
import nose

import saga.utils.test_config as sutc
from optparse import OptionParser

"""
This script runs a set of unit tests, which are organized in a directory
structure under ``tests/unittests/`` -- each sub-directory in that hierarchy
represents a test suite to be run.

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


#-----------------------------------------------------------------------------
#
def launch_tests(options, testdir):

    # test_cfgs will contain a list of all configuation files
    # that we will use for the tests
    test_cfgs = []

    for config in options.config.split(","):
        if os.path.exists(config):
            if os.path.isdir(config):
                test_cfgs = test_cfgs + glob.glob(config + "/*.cfg")
            else:
                test_cfgs.append(config)
        else:
            print "ERROR: Directory/file '%s' doesn't exist." % config
            return -1

    print "Running the following configurations:"
    for test_cfg in test_cfgs:
        print " * %s" % test_cfg

    # by default run all tests
    cfg_filters = [".*"]

    # the TestConfig singleton is shared with all test suites as they run
    tc = sutc.TestConfig()

    # tag the notimpl_warn_only option to the object
    tc.notimpl_warn_only = options.notimpl_warn_only

    # now cycle over the found test configs, configure the TestConfig accordingly,
    # and run all specified test_suites
    for sfg_name in test_cfgs:

        # only use this config if it matches the config filters.
        cfg_active = False
        for cfg_filter in cfg_filters:
            if re.search(cfg_filter, sfg_name):
                cfg_active = True
                break

        if not cfg_active:
            # this config file name did not match *any* of the given filters...
            print "skipping %s" % (os.path.basename(sfg_name))
            continue

        # initialize the correct test_util singleton
        # (i.e. with the correct configfile)
        tc.read_config(sfg_name)

        results = list()

        # run all test suites from the config
        for test_suite in tc.test_suites:

            # configure the unit test framework
            config = nose.config.Config()

            config.verbosity  = int(os.getenv('NOSE_VERBOSE', 1))
            config.workingDir = testdir + '/' + test_suite
            config.stream     = sys.stderr

            # and run tests
            print "______________________________________________________________________"
            print "%s : %s" % (os.path.basename (sfg_name), test_suite)
            rc = nose.core.run(config=config)
            results.append(rc)
            print "RC: %s" % rc
            print "______________________________________________________________________"

        # if we get a 'false' results it means that something went wrong. in
        # that case we return a non-zero exit code

        if False in results:
            return -1
        else:
            return 0


#-----------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--notimpl-warn-only",
                  action="store_true", dest="notimpl_warn_only", default=False,
                  help="if set, warn on NotImplemented exceptions. If set to \
'False' (default), NotImplemented will produce an error.")
    parser.add_option("-c", "--config", dest="config", metavar="CONFIG",
                  help="either a directory that contains test config files \
or a comma-separated list of individual config files")

    (options, args) = parser.parse_args()
    if options.config == None:
        print "ERROR: You need to provide the -c/--config option."
        sys.exit(-1)

    testdir = "%s/unittests/" % os.path.dirname(os.path.realpath(__file__))
    sys.exit(launch_tests(options=options, testdir=testdir))
