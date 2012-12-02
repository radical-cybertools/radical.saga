#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__     = "Ole Weidner"
__copyright__  = "Copyright 2012, The SAGA Project"
__license__    = "MIT"

''' Provides utilities to work with saga configuration files and query strings.
'''

class Configuration(object):
    
    def __init__(self):
        self._query_string = ""
        self._dictionary = {}

    def _re_eval(self, query_string=None):
        if query_string is not None:
            # initial eval from query string
            self._query_string = query_string
            for param in self._query_string.split('&'):
                try:
                    (key, value) = param.split("=")
                    self._dictionary[key] = value
                except ValueError:
                    if param != '':
                        self._dictionary[param] = ""
        else:
            # re-evaluate
            pass

    def as_query_string(self, subsection_name=None):
        ''' Return the configuration (or a subsection of it) as a URL 
            query string.
        '''
        self._re_eval()
        return self._query_string

    def as_config_file(self, subsection_name=None):
        ''' Return the configuration (or a subsection of it) as a  
            configuration file string.
        '''
        self._re_eval()

    def as_dict(self, subsection_name=None):
        ''' Return the configuration (or a subsection of it) as a
            Python dictionary.
        '''
        self._re_eval()
        return self._dictionary

class ConfigFile(Configuration):
    ''' Read and parse a saga configuration from a local file. 
    '''
    def __init__(self, file_path):
        Configuration.__init__(self)

class ConfigQuery(Configuration):
    ''' Parse a saga configuration from a URL query string.
    '''
    def __init__(self, query_string):
        Configuration.__init__(self)
        self._re_eval(query_string=query_string)



def _test_():
    cq = ConfigQuery("WhenToTransferOutput=ON_EXIT&should_transfer_files=YES&notifications=Always")
    print cq.as_dict()