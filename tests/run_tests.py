
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import sys
import glob
import optparse

try:
    import saga
    import saga.utils.test_config as sutc
    import radical.utils.testing  as rut
    import radical.utils          as ru

    dh = ru.DebugHelper()

    print "____________________________________________________________________"
    print "Using saga-python from: %s" % str(saga)
    print "____________________________________________________________________"

except Exception, e:
    srcdir = "%s/../" % os.path.dirname(os.path.realpath(__file__))
    sys.path.insert(0, os.path.abspath(srcdir))
    import saga
    import saga.utils.test_config as sutc
    import radical.utils.testing  as rut
    print "____________________________________________________________________"
    print "Using saga-python from: %s" % str(saga)
    print "____________________________________________________________________"


# ------------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print "ERROR: provide test config files as arguments"
        sys.exit (-1)


    test_cfgs = sys.argv[1:]

    # set up the testing framework
    testing = rut.Testing ('saga', __file__)
    ret     = True
    for test_cfg in test_cfgs :

        # use this config, ignore not implemented errors, run tests
        tc = sutc.TestConfig(test_cfg)
        tc.notimpl_warn_only = True

        if not testing.run(tc):
            ret = False

    sys.exit (ret)


# ------------------------------------------------------------------------------


