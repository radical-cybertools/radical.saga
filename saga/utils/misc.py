
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

_benchmark_times  = []
_benchmark_start  = 0.0
_benchmark_idx    = 0
_benchmark_notes  = []


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
def benchmark_start (notes=['benchmark']) :

    global _benchmark_notes
    global _benchmark_start
    global _benchmark_times
    global _benchmark_idx

    _benchmark_notes = notes
    _benchmark_start = time.time()
    _benchmark_times = []
    _benchmark_idx   = 0

    print "\nBenchmark: %s" % ", ".join (notes)


# --------------------------------------------------------------------
#
def benchmark_tic () :

    global _benchmark_notes
    global _benchmark_start
    global _benchmark_times
    global _benchmark_idx

    now = time.time ()

    _benchmark_times.append (now - _benchmark_start)
    _benchmark_start = now

    if   not ( (_benchmark_idx)        ) : sys.stdout.write ('*')
    elif not ( (_benchmark_idx) % 1000 ) : sys.stdout.write ('\n#')
    elif not ( (_benchmark_idx) %  100 ) : sys.stdout.write ('\n|')
    elif not ( (_benchmark_idx) %   10 ) : sys.stdout.write (':')
    else                                 : sys.stdout.write ('.')

    sys.stdout.flush ()

    _benchmark_idx += 1

# --------------------------------------------------------------------
#
def benchmark_eval () :

    global _benchmark_notes
    global _benchmark_start
    global _benchmark_times
    global _benchmark_idx


    if  len(_benchmark_times) <= 4 :

        raise Exception ("min 4 timing values required for benchmark evaluation")


    out = "\n"
    top = ""
    tab = ""

    out += "Benchmark results:\n"

    for note in _benchmark_notes :
        out += "  - %s\n" % note

    vn    = len (_benchmark_times) - 1
    vsum  = sum (_benchmark_times[1:])
    vmin  = min (_benchmark_times[1:])
    vmax  = max (_benchmark_times[1:])
    vmean = sum (_benchmark_times[1:]) / vn
    vsdev = math.sqrt (sum ((x - vmean) ** 2 for x in _benchmark_times[1:]) / vn)
    vrate = vn / vsum

    out += "  n     : %5.0d\n" % vn
    out += "  init  : %8.2fs\n" % _benchmark_times[0]
    out += "  1     : %8.2fs\n" % _benchmark_times[1]
    out += "  2     : %8.2fs\n" % _benchmark_times[2]
    out += "  3     : %8.2fs\n" % _benchmark_times[3]
    out += "  sum   : %8.2fs\n" % vsum
    out += "  min   : %8.2fs\n" % vmin
    out += "  max   : %8.2fs\n" % vmax
    out += "  mean  : %9.3fs\n" % vmean
    out += "  sdev  : %9.3fs\n" % vsdev
    out += "  rate  : %9.3fs\n" % vrate

    top = "%8s  %7s  %7s  %7s  %7s  " \
          "%7s  %7s  %7s  %8s %8s  %8s  %s" \
        % ('n', 'init', 'time.1', 'time.2', 'time.3', \
           'sum', 'min',  'max', 'mean', 'std-dev', 'rate', 'notes')

    tab = "%8d  %7.2f  %7.2f  %7.2f  %7.2f  " \
          "%7.2f  %7.2f  %7.2f  %8.3f %8.3f %8.3f  '%s'" \
        % (vn, _benchmark_times[0], _benchmark_times[1], _benchmark_times[2], _benchmark_times[3], \
           vsum,   vmin,  vmax, vmean, vsdev, vrate, ",".join (_benchmark_notes))

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
        f.write ("%s\n" % top)
    f.write ("%s\n" % tab)
    print

#
# --------------------------------------------------------------------

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

