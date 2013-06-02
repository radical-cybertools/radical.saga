
import os
import sys

import saga.utils.misc as sumisc

# ------------------------------------------------------------------------------
#
def benchmark_pre (test_cfg, bench_cfg, session) :

    pass


# ------------------------------------------------------------------------------
#
def benchmark_core (args={}) :

    pass


# ------------------------------------------------------------------------------
#
def benchmark_post (args={}) :

    pass


# ------------------------------------------------------------------------------
#
try:

    sumisc.benchmark_init ('benchmark.selftest', benchmark_pre, benchmark_core, benchmark_post)

except Exception as e :

    print "Exception: %s" % e


