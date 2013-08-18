
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import saga.utils.threads as sut

_singleton_lock = sut.RLock ('singleton creation lock')


""" Provides a Singleton metaclass.  """

# ------------------------------------------------------------------------------
#
class Singleton (type):
    """ A metaclass to 'tag' other classes as singleton::

            from saga.utils.singleton import Singleton
            class MyClass(BaseClass):
                __metaclass__ = Singleton
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):

      # with _singleton_lock :
        if True :

            if cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

            return cls._instances[cls]

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

