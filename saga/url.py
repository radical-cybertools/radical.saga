# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, The SAGA Project"
__license__   = "MIT"

# Using urlparse from Python 2.5
from saga.exceptions import BadParameter
from saga.contrib import urlparse25 as urlparse  

class Url(object):
    """ The Url class as defined in GFD.90.
    
        URLs are used in several places in the SAGA API: to specify service
        endpoints for job submission or resource management, for file or 
        directory locations, etc.  

        The URL class is designed to simplify URL management for these
        purposes -- it allows to manipulate individual URL elements, while 
        ensuring that the resulting URL is well formatted. Example::

        # create a URL from user input
        location = saga.Url (sys.argv[1])

        d = saga.filesystem.Directory(location)


        A URL consists of the following components (where one ore more can 
        be 'None')::

        <scheme>://<user>:<pass>@<host>:<port>/<path>?<query>#<fragment>

        Each of these components can be accessed via its property or 
        alternatively, via getter / setter methods. Example::

        url = saga.Url ("scheme://pass:user@host:123/dir/file?query#fragment")
  
        # modify the scheme 
        url.scheme = "anotherscheme"
        print "host: %s" % url.host
        print "scheme: %s" % url.scheme
        print "path: %s" % url.path
       
        # above is equivalent with  

        url.set_scheme("anotherscheme")
        print "host: %s" % url.get_host()
        print "scheme: %s" % url.get_scheme()
        print "path: %s" % url.get_path()
    """

    ######################################################################
    ##
    def __init__(self, url_string=''):
        '''Create a new Url object from a string or another Url object.'''

        if type(url_string) == str:
            self._urlobj = urlparse.urlparse(url_string)    
        elif type(url_string) == Url:
            self._urlobj = urlparse.urlparse(str(url_string))        
        else:
            raise BadParameter("Url expects str or Url type as parameter")

    ######################################################################
    ##
    def __str__(self):
        """String representation (utf-8)."""
        return unicode(self).decode('utf-8', 'ignore')

    ######################################################################
    ##
    def __unicode__(self):
        """Unicode representation."""
        ucstring = u'%s' % unicode(self._urlobj.geturl())
        return ucstring

    ######################################################################
    ##
    def _make_netloc(self, username, password, host, port):
        """Private helper function to generate netloc string"""
        netloc = str()
        if username is not None:
            if password is not None:
                netloc += "%s:%s@" % (username, password)
            else:
                netloc += "%s@" % (username)
        if host is not None:
            netloc += host
        if port is not None:
            netloc += ":%s" % (port)
        return netloc                

    ######################################################################
    ## Scheme property
    def set_scheme(self, scheme):
        """Set the 'scheme' component of the URL.
        """
        newurl = urlparse.urlunparse((scheme, 
                                     self._urlobj.netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_scheme(self):
        """Return the 'scheme' component of the URL.
           @return: str
        """
        return self._urlobj.scheme

    scheme=property(get_scheme, set_scheme)
    """The scheme component of the URL.
       @type: str
    """

    ######################################################################
    ## Host property
    def set_host(self, host):
        """Set the 'host' component of the URL.
        """
        netloc = self._make_netloc(self._urlobj.username, self._urlobj.password,
                                   host, self._urlobj.port)

        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_host(self):
        """Return the 'host' component of the URL.
           @return: str
        """
        return self._urlobj.hostname

    host=property(get_host, set_host)
    """The host component of the URL.
       @type: str
    """

    ######################################################################
    ## Port property
    def set_port(self, port):
        """Set the 'port' component of the URL.
        """
        netloc = self._make_netloc(self._urlobj.username, self._urlobj.password,
                                   self._urlobj.hostname, int(port))

        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_port(self):
        """Return the 'port' component of the URL.
           @return: int
        """
        if self._urlobj.port is not None:
            return int(self._urlobj.port)
        else:
            return None

    port=property(get_port, set_port)
    """The port component of the URL.
       @type: int
    """

    ######################################################################
    ## Username property
    def set_username(self, username):
        """Set the 'username' component of the URL.
           @type username: str
        """
        netloc = self._make_netloc(username, self._urlobj.password,
                                   self._urlobj.hostname, self._urlobj.port)

        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_username(self):
        """Return the 'username' component of the URL.
           @return: str
        """
        return self._urlobj.username

    username=property(get_username, set_username)
    """The username component of the URL.
       @type: str
    """


    ######################################################################
    ## Password property
    def set_password(self, password):
        """Set the 'password' component of the URL.
           @type password: str
        """
        netloc = self._make_netloc(self._urlobj.username, password,
                                   self._urlobj.hostname, self._urlobj.port)

        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_password(self):
        """Return the 'username' component of the URL.
           @return: str
        """
        return self._urlobj.password

    password=property(get_password, set_password)
    """The password component of the URL.
       @type: str
    """

    ######################################################################
    ## Fragment property
    def set_fragment(self, fragment):
        """Set the 'fragment' component of the URL.
           @type fragment: str
        """
        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     self._urlobj.netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_fragment(self):
        """Return the 'fragment' component of the URL.
           @return: str
        """
        return self._urlobj.fragment

    fragment=property(get_fragment, set_fragment)
    """The fragment component of the URL.
       @type: str
    """

    ######################################################################
    ## Path property
    def set_path(self, path):
        """Set the 'path' component of the URL.
           @type path: str
        """
        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     self._urlobj.netloc, 
                                     path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_path(self):
        """Return the 'path' component of the URL.
           @return: str
        """
        if '?' in self._urlobj.path:
            (path, query) = self._urlobj.path.split('?')
            return path
        else:
            return self._urlobj.path

    path=property(get_path, set_path)
    """The path component of the URL.
       @type: str
    """

    ######################################################################
    ## Query property
    def set_query(self, path):
        """Set the 'query' component of the URL.
           @type path: str
        """
        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     self._urlobj.netloc, 
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    def get_query(self):
        """Return the 'query' component of the URL.
           @return: str
        """
        if self._urlobj.query == '':
            if '?' in self._urlobj.path:
                (path, query) = self._urlobj.path.split('?')
                return query
        else:
            return self._urlobj.query

    query=property(get_query, set_query)
    """The query component of the URL.
       @type: str
    """
