
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
#
class Url (ru.Url) :
    """ 
    The SAGA Url class.

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
    pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

