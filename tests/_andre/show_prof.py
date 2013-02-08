
import pstats

p = pstats.Stats('test_perf.prof')
p.sort_stats('cumulative').print_stats()


