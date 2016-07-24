
from __future__ import absolute_import
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import pstats

p = pstats.Stats('test_perf.prof')
p.sort_stats('cumulative').print_stats()


