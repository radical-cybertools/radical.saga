
import saga.utils.benchmark as sb

import os
import sys



# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, test_cfg, bench_cfg, session) :

    pass


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, args={}) :

    pass


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, args={}) :

    pass


# ------------------------------------------------------------------------------
#
try:
    sb.benchmark_init ('benchmark_selftest', benchmark_pre, benchmark_core, benchmark_post)

except Exception as e :
    print "Exception: %s" % e




