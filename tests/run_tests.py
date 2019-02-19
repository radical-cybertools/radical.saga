
import os
import sys
import glob
import optparse

try:
    import radical.saga                   as rs
    import radical.saga.utils.test_config as sutc
    import radical.utils.testing          as rut
    import radical.utils                  as ru

    dh = ru.DebugHelper()

    print "______________________________________________________________________"
    print "Using radical.saga from: %s" % str(rs)
    print "______________________________________________________________________"

except Exception, e:
    srcdir = "%s/../" % os.path.dirname(os.path.realpath(__file__))
    sys.path.insert(0, os.path.abspath(srcdir))
    import radical.saga                   as rs
    import radical.saga.utils.test_config as sutc
    import radical.utils.testing          as rut
    print "______________________________________________________________________"
    print "Using radical.saga from: %s" % str(rs)
    print "______________________________________________________________________"


# ------------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print "ERROR: provide test config files as arguments"
        sys.exit (-1)


    test_cfgs = sys.argv[1:]

    # set up the testing framework
    testing = rut.Testing ('radical.saga', __file__)
    ret     = True
    for test_cfg in test_cfgs :

        # use this config, ignore not implemented errors, run tests
        tc = sutc.TestConfig(test_cfg)
        tc.notimpl_warn_only = True

        if not testing.run(tc):
            ret = False

    sys.exit (ret)


# ------------------------------------------------------------------------------


