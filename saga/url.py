
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os


# this urlparse needs Python 2.5
from saga.utils.contrib import urlparse25 as urlparse

import saga.exceptions       as se
import saga.utils.signatures as sus

# ------------------------------------------------------------------------------
#
class Url (object):
    """ The SAGA Url class.

        URLs are used in several places in the SAGA API: to specify service
        endpoints for job submission or resource management, for file or
        directory locations, etc.

        The URL class is designed to simplify URL management for these
        purposes -- it allows to manipulate individual URL elements, while
        ensuring that the resulting URL is well formatted. Example::

          # create a URL from a string
          location = saga.Url ("file://localhost/tmp/file.dat")
          d = saga.filesystem.Directory(location)

        A URL consists of the following components (where one ore more can
        be 'None')::

          <scheme>://<user>:<pass>@<host>:<port>/<path>?<query>#<fragment>

        Each of these components can be accessed via its property or
        alternatively, via getter / setter methods. Example::

          url = saga.Url ("scheme://pass:user@host:123/path?query#fragment")

          # modify the scheme
          url.scheme = "anotherscheme"

          # above is equivalent with
          url.set_scheme("anotherscheme")
    """

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Url', 
                  sus.optional (basestring, 'Url'))
    @sus.returns (sus.nothing)
    def __init__(self, url_string=''):
        """ Create a new Url object from a string or another Url object.
        """

        if type(url_string) == type(None):
            self._urlobj = urlparse.urlparse("")
        if type(url_string) == str:
            self._urlobj = urlparse.urlparse(url_string)
        elif type(url_string) == Url:
            self._urlobj = urlparse.urlparse(str(url_string))
        else:
            raise se.BadParameter ("Url expects str or Url type as parameter, not %s" \
                                % type(url_string))

    # --------------------------------------------------------------------------
    #
    ##
    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def __str__  (self):
        """ String representation (utf-8).
        """
        return unicode(self).decode('utf-8', 'ignore')

    # --------------------------------------------------------------------------
    #
    ##
    @sus.takes   ('Url')
    @sus.returns (basestring)
    def __unicode__(self):
        """ Unicode representation.
        """
        ucstring = u'%s' % unicode(self._urlobj.geturl())
        return ucstring

    # --------------------------------------------------------------------------
    #
    ##
    @sus.takes   ('Url', 
                  dict)
    @sus.returns ('Url')
    def __deepcopy__(self, memo):
        """ Deep copy of a Url
        """
        new_url = Url(str(self))
        return new_url

    # --------------------------------------------------------------------------
    #
    ##
    @sus.takes   ('Url', 
                  basestring,
                  basestring,
                  basestring,
                  (basestring, int))
    @sus.returns (basestring)
    def _make_netloc(self, username, password, host, port):
        """ Private helper function to generate netloc string.
        """
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

    # --------------------------------------------------------------------------
    #
    # Scheme property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_scheme(self, scheme):
        """ Set the 'scheme' component.
        """
        newurl = urlparse.urlunparse((scheme,
                                     self._urlobj.netloc,
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_scheme(self):
        """R eturn the 'scheme' component.
        """
        return self._urlobj.scheme

    scheme = property(get_scheme, set_scheme)
    schema = scheme  # alias, as both terms are used...
    """ The scheme component.
    """

    # --------------------------------------------------------------------------
    #
    # Host property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_host(self, host):
        """ Set the 'host' component.
        """
        netloc = self._make_netloc(self._urlobj.username,
                                   self._urlobj.password,
                                   host, self._urlobj.port)

        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     netloc,
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_host(self):
        """ Return the 'host' component.
        """
        return self._urlobj.hostname

    host = property(get_host, set_host)
    """ The host component.
    """

    # --------------------------------------------------------------------------
    #
    # Port property
    #
    @sus.takes   ('Url', 
                  (basestring, int))
    @sus.returns (sus.nothing)
    def set_port(self, port):
        """ Set the 'port' component.
        """
        netloc = self._make_netloc(self._urlobj.username,
                                   self._urlobj.password,
                                   self._urlobj.hostname, int(port))

        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     netloc,
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, int))
    def get_port(self):
        """ Return the 'port' component.
        """
        if self._urlobj.port is not None:
            return int(self._urlobj.port)
        else:
            return None

    port = property(get_port, set_port)
    """ The port component.
    """

    # --------------------------------------------------------------------------
    #
    # Username property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_username(self, username):
        """ Set the 'username' component.
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

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_username(self):
        """ Return the 'username' component.
        """
        return self._urlobj.username

    username = property(get_username, set_username)
    """ The username component.
    """

    # --------------------------------------------------------------------------
    #
    # Password property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_password(self, password):
        """ Set the 'password' component.
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

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_password(self):
        """ Return the 'username' component.
        """
        return self._urlobj.password

    password = property(get_password, set_password)
    """ The password component.
    """

    # --------------------------------------------------------------------------
    #
    # Fragment property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_fragment(self, fragment):
        """ Set the 'fragment' component.
        """
        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     self._urlobj.netloc,
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     fragment))
        self._urlobj = urlparse.urlparse(newurl)

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_fragment(self):
        """ Return the 'fragment' component.
        """
        return self._urlobj.fragment

    fragment = property(get_fragment, set_fragment)
    """ The fragment component.
    """

    # --------------------------------------------------------------------------
    #
    # Path property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_path(self, path):
        """ Set the 'path' component.
        """
        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     self._urlobj.netloc,
                                     path,
                                     self._urlobj.params,
                                     self._urlobj.query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_path(self):
        """ Return the 'path' component.
        """
        if '?' in self._urlobj.path:
            (path, query) = self._urlobj.path.split('?')
            return os.path.normpath(path)
        else:
            return os.path.normpath(self._urlobj.path)

    path = property(get_path, set_path)
    """ The path component.
    """

    # --------------------------------------------------------------------------
    #
    # Query property
    #
    @sus.takes   ('Url', 
                  basestring)
    @sus.returns (sus.nothing)
    def set_query(self, query):
        """ Set the 'query' component.
        """
        newurl = urlparse.urlunparse((self._urlobj.scheme,
                                     self._urlobj.netloc,
                                     self._urlobj.path,
                                     self._urlobj.params,
                                     query,
                                     self._urlobj.fragment))
        self._urlobj = urlparse.urlparse(newurl)

    @sus.takes   ('Url')
    @sus.returns ((sus.nothing, basestring))
    def get_query(self):
        """ Return the 'query' component.
        """
        if self._urlobj.query == '':
            if '?' in self._urlobj.path:
                (path, query) = self._urlobj.path.split('?')
                return query
        else:
            return self._urlobj.query

    query = property(get_query, set_query)
    """ The query component.
    """
