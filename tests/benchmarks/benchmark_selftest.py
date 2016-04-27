
import radical.utils.benchmark as rb

import os
import sys



# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
b = rb.Benchmark (dict(), 'benchmark_selftest', benchmark_pre, benchmark_core, benchmark_post)




