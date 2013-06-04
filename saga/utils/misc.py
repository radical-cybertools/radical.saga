
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import re
import os
import sys
import math
import time

import saga

""" Provides an assortment of utilities """

_benchmark = {}


# --------------------------------------------------------------------
#
def host_is_local (host) :
    """ Returns True if the given host is the localhost
    """
    
    import socket

    if  not host                   or \
        host == 'localhost'        or \
        host == socket.gethostname () :
        return True
    else :
        return False


# --------------------------------------------------------------------
#
def host_is_valid (host) :
    """ 
    Returns True if the given hostname can be resolved.
    We also test the reverse DNS lookup -- some seriously stupid and standard
    violating internet providers implement a DNS catchall -- the reverse lookup
    can catch that case in some cases (say that quickly 3 times!)
    """

    # FIXME: cache results so that further lookups are quick

    if  host_is_local (host) :
        return True
    
    import socket

    try :
        ip   = socket.gethostbyname (host)
        name = socket.gethostbyaddr (ip)
        return True
    except :
        return False


# --------------------------------------------------------------------
#
def url_is_local (arg) :
    """ Returns True if the given url points to localhost
    """
    
    import saga.url
    u = saga.url.Url (arg)

    return host_is_local (u.host)



# --------------------------------------------------------------------
#
def url_is_relative (url_1) :
    """ an URL is considered relative if it only contains a path element, and
    that path element does not start with '/'.
    """

    import saga.url

    u1 = saga.Url (url_1)

    if  str (u1) == str(u1.path) :
        if  u1.path and u1.path[0] != '/' :
            return True

    return False


# --------------------------------------------------------------------
#
def url_get_dirname (url_1) :
    """ 
    Extract the directory part of the given URL's path element.  We consider
    everything up to the last '/' as directory.  That also holds for relative
    paths.
    """

    import saga.url

    u1 = saga.Url (url_1)
    p1 = u1.path

    return re.sub (r"[^/]*$", "", p1)


# --------------------------------------------------------------------
#
def url_get_filename (url_1) :
    """ 
    Extract the directory part of the given URL's path element.  We consider
    everything up to the last '/' as directory.  That also holds for relative
    paths.
    """

    import saga.url

    u1 = saga.Url (url_1)
    p1 = u1.path

    if '/' in p1 :
        return re.sub (r"^.*/(.*)$", "\1", p1)
    else :
        return p1


# --------------------------------------------------------------------
#
def url_make_absolute (url_1, url_2) :
    """ 
    URL1 is expected to only have a path
    Missing elements in url_1 are copied from url_2 -- but path stays the
    same.  Unless it is a relative path in the first place: then it is
    interpreted as relative to url_2.path, and is made absolute.
    protocol/port/user etc.
    """

    import saga.url

    if not url_is_compatible (url_1, url_2) :
        raise saga.BadParameter ("Cannot interpret url %s in the context of url %s" \
                              % (url_2, url_1))

    # re-interpret path of url_2, using url_1 as base directory
    ret = saga.Url (url_1)

    if  url_is_relative (url_2) :
        # note that we have no means if 'file://localhost/tmp/test.txt' refers
        # to a file or an directory -- so we have to trust that url_1 is
        # a dir...
        ret.path = url_1.path + '/' + url_2.path
    else :
        # absolute path, replace url path...
        ret.path = url_2.path

    # FIXME: normalize, to get rid of double slashes etc.
    return ret


# --------------------------------------------------------------------
#
def url_is_compatible (url_1, url_2) :
    """ 
    Returns True if the given urls point to the same host, using the same
    protocol/port/user etc.  If one of the URLs only contains a path, it is
    considered compatible with any other URL.
    """
    
    import saga.url

    u1 = saga.Url (url_1)
    u2 = saga.Url (url_2)


    # if either one url only contains a path, it is compatible to anything.
    if u1.path == str(u1) : return True
    if u2.path == str(u2) : return True

    # more than path in both URLs -- check compatibility for all elements
    if u1.scheme   and     u2.scheme   and u1.scheme   != u2.scheme   : return False 
    if u1.host     and     u2.host     and u1.host     != u2.host     : return False
    if u1.port     and     u2.port     and u1.port     != u2.port     : return False
    if u1.username and     u2.username and u1.username != u2.username : return False
    if u1.password and     u2.password and u1.password != u2.password : return False

    if u1.scheme   and not u2.scheme                                  : return False 
    if u1.host     and not u2.host                                    : return False
    if u1.port     and not u2.port                                    : return False
    if u1.username and not u2.username                                : return False
    if u1.password and not u2.password                                : return False

    if u2.scheme   and not u1.scheme                                  : return False 
    if u2.host     and not u1.host                                    : return False
    if u2.port     and not u1.port                                    : return False
    if u2.username and not u1.username                                : return False
    if u2.password and not u1.password                                : return False

    # no differences detected (ignored fragments and query though)
    return True


# --------------------------------------------------------------------
#
def benchmark_init (name, func_pre, func_core, func_post) :
    
    _benchmark = {}

    s = saga.Session (default=True)

    # check if a config file was specified via '-c' command line option, and
    # read it, return the dict

    config_name = None

    for i, arg in enumerate (sys.argv[1:]) :
        if  arg == '-c' and len (sys.argv) > i+2 :
            config_name = sys.argv[i+2]


    if  not config_name :
        sumisc.benchmark_eval ('no configuration specified (-c <conf>')

    tc   = sutc.TestConfig ()
    tc.read_config (config_name)

    test_cfg  = tc.get_test_config ()
    bench_cfg = tc.get_benchmark_config ()
    session   = tc.session


    if  not 'concurrency' in bench_cfg : 
        sumisc.benchmark_eval ('no concurrency configured')

    if  not 'iterations'  in bench_cfg : 
        sumisc.benchmark_eval ('no iterations configured')


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

    try :
        t_cfg    = _benchmark['test_cfg']
        b_cfg    = _benchmark['bench_cfg']
        session  = _benchmark['session']

        pre      = b_cfg['pre']
        core     = b_cfg['core']
        post     = b_cfg['post']

        pre_ret  = pre (t_cfg, b_cfg, session)

        _benchmark['events'][tid]['event_1'].set  ()  # signal we are done        
        _benchmark['events'][tid]['event_2'].wait ()  # wait 'til others are done 

        iterations = int(b_cfg['iterations']) / int(b_cfg['concurrency'])

        for i in range (0, iterations) :
            core_ret = core (pre_ret)
            benchmark_tic (_benchmark)
>>>>>>> Stashed changes

    global _benchmark

    print "\nBenchmark : %s : %s" % (url, name)

    _url = saga.Url (url)

    _benchmark['url']   = _url 
    _benchmark['name']  = name
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

        import socket

        s = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
        s.connect ((host, port))

    except Exception as e :
        _benchmark['ping']  = -1.0
        sys.stdout.write ("no ping on %s:%s [%s]\n" % (host, port, e))

    else :
        _benchmark['ping']  = time.time () - ping_start
        sys.stdout.write ("%.5fs\n" % _benchmark['ping'])

    sys.stdout.flush ()



# --------------------------------------------------------------------
#
def benchmark_tic () :

    global _benchmark

    now   = time.time ()
    timer = now - _benchmark['start']

    _benchmark['times'].append (timer)
    _benchmark['start'] = now

    if len(_benchmark['times'][1:]) :
        vmean = sum (_benchmark['times'][1:]) / len(_benchmark['times'][1:])
    else :
        vmean = timer

    if   timer  <  0.75 * vmean : marker = '.'
    if   timer  <  0.90 * vmean : marker = ','
    elif timer  <  0.99 * vmean : marker = ':'
    elif timer  <  1.01 * vmean : marker = '*'
    elif timer  <  1.10 * vmean : marker = ';'
    elif timer  <  1.25 * vmean : marker = '-'
    else                        : marker = '='



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
def benchmark_eval () :

    global _benchmark

    if  len(_benchmark['times']) <= 4 :

        raise Exception ("min 4 timing values required for benchmark evaluation")


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

    out += "  url   : %s\n"                              % (_benchmark['url']            )
    out += "  ping  : %8.5fs          n     : %9d\n"     % (_benchmark['ping']    , vn   )
    out += "  init  : %8.2fs          min   : %8.2fs\n"  % (_benchmark['times'][0], vmin )
    out += "  1     : %8.2fs          max   : %8.2fs\n"  % (_benchmark['times'][1], vmax )
    out += "  2     : %8.2fs          mean  : %8.2fs\n"  % (_benchmark['times'][2], vmean)
    out += "  3     : %8.2fs          sdev  : %8.2fs\n"  % (_benchmark['times'][3], vsdev)
    out += "  sum   : %8.2fs          rate  : %8.2f/s\n" % (vsum,                   vrate)

    num = "# %5s  %7s  %7s  %7s  %7s  %7s" \
          "  %7s  %7s  %7s  %8s  %8s  %9s   %-18s   %s" \
        % (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    top = "# %5s  %7s  %7s  %7s  %7s  %7s" \
          "  %7s  %7s  %7s  %8s  %8s  %9s   %-18s   %s" \
        % ('ping', 'n', 'init', 'time.1', 'time.2', 'time.3', \
           'sum', 'min',  'max', 'mean', 'std-dev', 'rate', 'name', 'url')

    tab = "%7.5f  " \
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
           "'%s'" % _benchmark['name'],   # I am sorry, sooo sorry...  
           _benchmark['url'])

    print
    print out

    create_top = True
    try :
        statinfo = os.stat ('benchmark.dat')
        if statinfo.st_size > 0 :
            create_top = False
    except Exception :
        pass

    f = open ("benchmark.dat", "a+")

    if  create_top :
        f.write ("%s\n" % num)
        f.write ("%s\n" % top)
    f.write ("%s\n" % tab)

#
# --------------------------------------------------------------------

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

