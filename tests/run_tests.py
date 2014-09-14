
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
    import radical.utils.testing as rut

    print "______________________________________________________________________"
    print "Using saga-python from: %s" % str(saga)
    print "______________________________________________________________________"

except Exception, e:
    srcdir = "%s/../" % os.path.dirname(os.path.realpath(__file__))
    sys.path.insert(0, os.path.abspath(srcdir))

    import saga
    import saga.utils.test_config as sutc
    import radical.utils.testing  as rut

    print "______________________________________________________________________"
    print "Using saga-python from: %s" % str(saga)
    print "______________________________________________________________________"


#-----------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    configs = sys.argv[1:]

    if  not configs :
        print "ERROR: You need to provide test config files as arguments"
        sys.exit (-1)

    for config in configs :
        if  not os.path.exists (config) :
            print "ERROR: config '%s' does not exist." % config
            sys.exit (-1)


    # set up the testing framework
    testing = rut.Testing ('saga', __file__)
    ret     = 0

    for config in configs :

        # for each config, set up the test config singleton and run the tests
        tc = sutc.TestConfig (config)

        if  not testing.run () :
            ret += 1

    sys.exit (ret)


# ------------------------------------------------------------------------------


