
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
# entry point
if __name__ == "__main__":

    parser = optparse.OptionParser ()
    parser.add_option ("-n", "--notimpl-warn-only",
                       action  = "store_true", 
                       dest    = "notimpl_warn_only", 
                       default = False,
                       help    = "if set, warn on NotImplemented exceptions. "
                               + "If set to 'False' (default), NotImplemented "
                               + "will produce an error.")
    parser.add_option ("-c", "--config", 
                       dest    = "config", 
                       metavar = "CONFIG",
                       help    = "either a directory that contains test config "
                               + "files or a comma-separated list of individual "
                               + "config files")

    (options, args) = parser.parse_args ()

    if options.config == None:
        print "ERROR: You need to provide the -c/--config option."
        sys.exit (-1)


    # set up the testing framework
    testing = ru.Testing ('saga', __file__)

    # test_cfgs will contain a list of all configuation files
    # that we will use for the tests
    test_cfgs = []

    for config in options.config.split (",") :
        if  os.path.exists (config) :
            test_cfgs.append (config)
        else:
            print "ERROR: Directory/file '%s' doesn't exist." % config
            sys.exit (-1)


    for test_cfg in test_cfgs :

        # for each config, set up the test config singleton and run the tests
        tc = sutc.TestConfig (test_cfg)

        # tag the notimpl_warn_only option to the config
        tc.notimpl_warn_only = options.notimpl_warn_only

        testing.run ()


# ------------------------------------------------------------------------------


