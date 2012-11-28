#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__     = "Ole Weidner"
__copyright__  = "Copyright 2012, The SAGA Project"
__license__    = "MIT"

''' Provides a Singleton metaclass.
'''

class Singleton(type):
    ''' A metaclass to 'tag' other classes as singleton.

        Usage:
            from saga.utils.singleton import Singleton
            class MyClass(BaseClass):
                __metaclass__ = Singleton
    '''
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
