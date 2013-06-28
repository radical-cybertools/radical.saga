
import os
import sys
import math
import time
import socket

import saga.utils.threads     as sut
import saga.session           as sess
import saga.url               as surl
import saga.utils.test_config as sutc
import saga.utils.misc        as sumisc


# --------------------------------------------------------------------
#
def benchmark_init (name, func_pre, func_core, func_post) :

    
    _benchmark = {}

    s = sess.Session (default=True)
    sut.lout ('session was set up\n')

    # check if a config file was specified via '-c' command line option, and
    # read it, return the dict

    config_name = None

    for i, arg in enumerate (sys.argv[1:]) :
        if  arg == '-c' and len (sys.argv) > i+2 :
            config_name = sys.argv[i+2]


    if  not config_name :
        benchmark_eval (_benchmark, 'no configuration specified (-c <conf>')

    tc   = sutc.TestConfig ()
    tc.read_config (config_name)

    test_cfg  = tc.get_test_config ()
    bench_cfg = tc.get_benchmark_config ()
    session   = tc.session


    if  not 'concurrency' in bench_cfg : 
        benchmark_eval (_benchmark, 'no concurrency configured')

    if  not 'iterations'  in bench_cfg : 
        benchmark_eval (_benchmark, 'no iterations configured')

    if  not 'url' in bench_cfg :
        bench_cfg['url'] = ''

    _benchmark['url']       = bench_cfg['url']
    _benchmark['session']   = session
    _benchmark['test_cfg']  = test_cfg
    _benchmark['bench_cfg'] = bench_cfg

    _benchmark['bench_cfg']['pre']   = func_pre
    _benchmark['bench_cfg']['core']  = func_core
    _benchmark['bench_cfg']['post']  = func_post
    _benchmark['bench_cfg']['name']  = name
    _benchmark['bench_cfg']['cname'] = config_name

    benchmark_run  (_benchmark)
    benchmark_eval (_benchmark)

# ------------------------------------------------------------------------------
#
def benchmark_thread (tid, _benchmark) :

    t_cfg    = _benchmark['test_cfg']
    b_cfg    = _benchmark['bench_cfg']
    session  = _benchmark['session']
    lock     = _benchmark['lock']

    pre      = b_cfg['pre']
    core     = b_cfg['core']
    post     = b_cfg['post']

    try :
      pre_ret  = pre (t_cfg, b_cfg, session)
      sys.stdout.write ('-')
      sys.stdout.flush ()


      _benchmark['events'][tid]['event_1'].set  ()  # signal we are done        
      _benchmark['events'][tid]['event_2'].wait ()  # wait 'til others are done 

      iterations = int(b_cfg['iterations']) / int(b_cfg['concurrency'])


      for i in range (0, iterations) :
          core_ret = core (pre_ret)
          benchmark_tic   (_benchmark)


      _benchmark['events'][tid]['event_3'].set ()   # signal we are done        
      _benchmark['events'][tid]['event_4'].wait ()  # wait 'til others are done 


      post_ret = post (core_ret)
      sys.stdout.write ('=')
      sys.stdout.flush ()

      _benchmark['events'][tid]['event_5'].set ()   # signal we are done        

    except Exception as e :

      # Oops, we are screwed.  Tell main thread that wer are done for, and
      # bye-bye...
      _benchmark['events'][tid]['event_1'].set  ()  # signal we are done        
      _benchmark['events'][tid]['event_3'].set  ()  # signal we are done        
      _benchmark['events'][tid]['event_5'].set  ()  # signal we are done        

      print sumisc.get_exception_traceback_str  ()

      sys.stdout.write ("exception in benchmark thread: %s\n\n" % e)
      sys.stdout.flush ()
      sys.exit (-1)

    sys.exit (0)


# ------------------------------------------------------------------------------
#
def benchmark_run (_benchmark) :
    """
    - create 'concurrency' number of threads
    - per thread call pre()
    - sync threads, start timer
    - per thread call core() 'iteration' number of times', tic()
    - stop timer
    - per thread, call post, close threads
    - eval once
    """

    cfg         = _benchmark['bench_cfg']
    threads     = []
    concurrency = int(_benchmark['bench_cfg']['concurrency'])

    benchmark_start (_benchmark)

    _benchmark['events'] = {}

    for tid in range (0, concurrency) :

        _benchmark['events'][tid] = {}
        _benchmark['events'][tid]['event_1'] = sut.Event ()
        _benchmark['events'][tid]['event_2'] = sut.Event ()
        _benchmark['events'][tid]['event_3'] = sut.Event ()
        _benchmark['events'][tid]['event_4'] = sut.Event ()
        _benchmark['events'][tid]['event_5'] = sut.Event ()

        t = sut.SagaThread (benchmark_thread, tid, _benchmark)
        threads.append (t)


    for t in threads :
        t.start ()

    
    # wait for all threads to start up and initialize
    sut.lout ("\n> " + "="*concurrency)
    sut.lout ("\n> ")
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_1'].wait ()

    benchmark_tic (_benchmark)

    # start workload in all threads
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_2'].set ()

    # wait for all threads to finish core test
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_3'].wait ()

    # start shut down
    sut.lout ("\n< " + "-"*concurrency)
    sut.lout ("\n< ")
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_4'].set ()

    # wait for all threads to finish shut down
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_5'].wait ()



# --------------------------------------------------------------------
#
def benchmark_start (_benchmark) :

    cfg = _benchmark['bench_cfg']

    sut.lout ("\nBenchmark : %s : %s\n" % (cfg['name'], cfg['url']))

    _url = surl.Url (cfg['url'])
    lock = sut.RLock ()

    _benchmark['lock']  = lock
    _benchmark['url']   = _url 
    _benchmark['start'] = time.time()
    _benchmark['times'] = []
    _benchmark['idx']   = 0

    if _url.host : host = _url.host
    else         : host = 'localhost'

    if _url.port : port = _url.port
    else         : port = 22  #  FIXME: we should guess by protocol 

    ping_start = time.time ()

    try :
        sys.stdout.write ('Latency   : ')
        sys.stdout.flush ()

        s = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
        s.connect  ((host, port))
        s.shutdown (socket.SHUT_RDWR)

    except Exception as e :
        _benchmark['ping']  = -1.0
        sys.stdout.write ("no ping on %s:%s [%s]\n" % (host, port, e))

    else :
        _benchmark['ping']  = time.time () - ping_start
        sys.stdout.write ("%.5fs\n" % _benchmark['ping'])

    sys.stdout.flush ()



# --------------------------------------------------------------------
#
def benchmark_tic (_benchmark) :

    with _benchmark['lock'] :

        now   = time.time ()
        timer = now - _benchmark['start']

        _benchmark['times'].append (timer)
        _benchmark['start'] = now

        if len(_benchmark['times'][1:]) :
            vmean = sum (_benchmark['times'][1:]) / len(_benchmark['times'][1:])
        else :
            vmean = timer

        if   timer  <  0.75 * vmean : marker = '-'
        if   timer  <  0.90 * vmean : marker = '_'
        elif timer  <  0.95 * vmean : marker = '.'
        elif timer  <  1.05 * vmean : marker = '*'
        elif timer  <  1.10 * vmean : marker = ':'
        elif timer  <  1.25 * vmean : marker = '='
        else                        : marker = '+'


        if       not ( (_benchmark['idx'] - 1)        ) : sys.stdout.write ('\n* ')
        else :
            if   not ( (_benchmark['idx'] - 1) % 1000 ) : sys.stdout.write ('\n\n# ')
            elif not ( (_benchmark['idx'] - 1) %  100 ) : sys.stdout.write ('\n| ')
            elif not ( (_benchmark['idx'] - 1) %   10 ) : sys.stdout.write (' ')

        if           ( (_benchmark['idx']    )        ) : sys.stdout.write (marker)
        
        sys.stdout.flush ()

        _benchmark['idx'] += 1

# --------------------------------------------------------------------
#
def benchmark_eval (_benchmark, error=None) :

    if  error :
        sut.lout ("\nBenchmark error: %s\n" % error)
        sys.exit (-1)


    if  len(_benchmark['times']) <= 4 :
        raise Exception ("min 4 timing values required for benchmark evaluation")

    concurrency = int(_benchmark['bench_cfg']['concurrency'])

    out = "\n"
    top = ""
    tab = ""
    num = ""

    out += "Results :\n"

    vn    = len (_benchmark['times']) - 1
    vsum  = sum (_benchmark['times'][1:])
    vmin  = min (_benchmark['times'][1:])
    vmax  = max (_benchmark['times'][1:])
    vmean = sum (_benchmark['times'][1:]) / vn
    vsdev = math.sqrt (sum ((x - vmean) ** 2 for x in _benchmark['times'][1:]) / vn)
    vrate = vn / vsum

    out += "  url     : %s\n"                                % (_benchmark['url']            )
    out += "  ping    : %8.5fs\n"                            % (_benchmark['ping']           )
    out += "  n       : %9d          total   : %8.2fs\n"     % (vn, vsum                     )
    out += "  threads : %9d          min     : %8.2fs\n"     % (concurrency           , vmin )
    out += "  init    : %8.2fs          max     : %8.2fs\n"  % (_benchmark['times'][0], vmax )
    out += "  1       : %8.2fs          mean    : %8.2fs\n"  % (_benchmark['times'][1], vmean)
    out += "  2       : %8.2fs          sdev    : %8.2fs\n"  % (_benchmark['times'][2], vsdev)
    out += "  3       : %8.2fs          rate    : %8.2f/s\n" % (_benchmark['times'][3], vrate)

    num = "# %5s  %7s  %7s  %7s  %7s  %7s  %7s" \
          "  %7s  %7s  %7s  %8s  %8s  %9s   %-18s   %s" \
        % (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
    top = "# %5s  %7s  %7s  %7s  %7s  %7s  %7s" \
          "  %7s  %7s  %7s  %8s  %8s  %9s   %-18s   %s" \
        % ('ping', 'n', 'threads', 'init', 'time.1', 'time.2', 'time.3', \
           'sum', 'min',  'max', 'mean', 'std-dev', 'rate', 'name', 'url')

    tab = "%7.5f  " \
          "%7d  "   \
          "%7d  "   \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%8.3f  " \
          "%8.3f  " \
          "%9.3f  " \
          "%-20s  " \
          "%s"      \
        % (_benchmark['ping'], 
           vn, 
           concurrency, 
           _benchmark['times'][0],      
           _benchmark['times'][1], 
           _benchmark['times'][2], 
           _benchmark['times'][3],
           vsum,   
           vmin,  
           vmax, 
           vmean, 
           vsdev, 
           vrate, 
           "'%s'" % _benchmark['bench_cfg']['name'],   # I am sorry, sooo sorry...  
           _benchmark['url'])

    sut.lout ("\n%s" % out)

    create_top = True
    statinfo   = os.stat ('benchmark.dat')
    if  statinfo.st_size > 0 :
        create_top = False

    f = open ("benchmark.dat", "a+")

    if  create_top :
        f.write ("%s\n" % num)
        f.write ("%s\n" % top)
    f.write ("%s\n" % tab)

# --------------------------------------------------------------------


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

