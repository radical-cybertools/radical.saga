# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides API handles for SAGA's logging system.
'''

from saga.utils.singleton import Singleton

class Config(object): # This should inherit from some sort of a Config base class
    ''' Represents the logging system configuration.

        The Config class can be used to introspect and modify the configuration
        of SAGA's logging system. Changes take place immediately during 
        application execution and override logging system specific environment 
        variables set in the shell.
    '''
    __metaclass__ = Singleton

    def __init__(self):
        pass

    def get_registered_logger_names(self):
        ''' Returns the list of names of currently registered loggers.
        '''

    def filters():  
        ''' The logger name filter property. 
               
            Takes and returns a set of strings representing logger names that
            should be included in the logging output.
        '''
        def fget(self):
            pass
        def fset(self, value):
            pass
        def fdel(self):
            pass
        return locals()
    filters = property(**filters())

    def targets():
        ''' The log targets property.

            Takes and returns a set of strings representing target names.
            Target names can either be a path and filename or the keyword
            ``STDOUT`` (string, case-insensitive) for console output.
        '''
        def fget(self):
            pass
        def fset(self, value):
            pass
        def fdel(self):
            pass
        return locals()
    targets = property(**targets())

    def level():
        ''' The log level property.

            Takes and returns a numeric (0-4) value or a string 
            (case-insensitive) representing the log level. Valid string
            values are ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``.
        '''
        def fget(self):
            pass
        def fset(self, value):
            pass
        def fdel(self):
            pass
        return locals()
    level = property(**level())

def getConfig():
    ''' Return the logging system configuration.

        All calls to this function return the same Config instance. This means 
        that Config instances never need to be passed between different parts
        of an application. 
    '''
    return Config() 

