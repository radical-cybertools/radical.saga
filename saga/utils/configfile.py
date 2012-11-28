#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__     = "Ole Weidner"
__copyright__  = "Copyright 2012, The SAGA Project"
__license__    = "MIT"

''' Provides utilities to work with saga configuration files and query strings.
'''

class Configuration(object):
    
    def __init__(self):
        pass

    def as_query_string(self, subsection_name=None):
        ''' Return the configuration (or a subsection of it) as a URL 
            query string.
        '''
        pass

    def as_config_file(self, subsection_name=None):
        ''' Return the configuration (or a subsection of it) as a  
            configuration file string.
        '''
        pass

class ConfigFile(Configuration):
    ''' Read and parse a saga configuration from a local file. 
    '''
    def __init__(self, file_path):
        pass

class ConfigQuery(Configuration):
    ''' Parse a saga configuration from a URL query string.
    '''
    def __init__(self, query_string):
        pass



