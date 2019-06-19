
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

    print("____________________________________________________________________")
    print("Using radical.saga from: %s" % str(rs))
    print("____________________________________________________________________")

except Exception as e:
    srcdir = "%s/../" % os.path.dirname(os.path.realpath(__file__))
    sys.path.insert(0, os.path.abspath(srcdir))
    import radical.saga                   as rs
    import radical.saga.utils.test_config as sutc
    import radical.utils.testing          as rut

    print("____________________________________________________________________")
    print("Using radical.saga from: %s" % str(rs))
    print("____________________________________________________________________")


# ------------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("ERROR: provide test config files as arguments")
        sys.exit (-1)

    configs = sys.argv[1:]

    for config in configs :
        if  not os.path.exists (config) :
            print("ERROR: config '%s' does not exist." % config)
            sys.exit (-1)

    test_cfgs = sys.argv[1:]

    # set up the testing framework
    testing = rut.Testing ('radical.saga', __file__)
    ret     = 0

    for config in configs :

        # for each config, set up the test config singleton and run the tests
        tc = sutc.TestConfig (config)

        if  not testing.run () :
            ret += 1

    sys.exit (ret)


# ------------------------------------------------------------------------------


