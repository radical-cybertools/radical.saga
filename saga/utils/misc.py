
""" Provides an assortment of utilities """

# --------------------------------------------------------------------
#
def host_is_local (host) :
    """ Returns True if the given host is the localhost
    """
    
    import socket

    if host == 'localhost'        or \
       host == socket.gethostname () :
        return True
    else :
        return False


def url_is_local (arg) :
    """ Returns True if the given url points to localhost
    """
    
    import saga.url
    u = saga.url.Url (arg)

    if not u.host :
        return True

    return host_is_local (u.host)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

