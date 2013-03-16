
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Provides an assortment of utilities """

# --------------------------------------------------------------------
#
def host_is_local (host) :
    """ Returns True if the given host is the localhost
    """
    
    # FIXME: cache results so that further lookups are quick

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



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

