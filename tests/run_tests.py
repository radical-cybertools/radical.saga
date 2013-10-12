
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import sys
import glob
import optparse

import saga.utils.test_config as sutc
import radical.utils as ru

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

    testing = ru.Testing ('saga')


    print "RUNNING the following configurations:"
    for test_cfg in test_cfgs:
        print " * %s" % test_cfg
        

    for test_cfg in test_cfgs :

        tc = sutc.TestConfig (test_cfg)

        # # tag the notimpl_warn_only option to the object
        # tc.notimpl_warn_only = options.notimpl_warn_only

        testing.run (tc)
        




#-----------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    parser = optparse.OptionParser()
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

