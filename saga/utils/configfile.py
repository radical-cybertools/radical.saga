#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__     = "Ole Weidner"
__copyright__  = "Copyright 2012, The SAGA Project"
__license__    = "MIT"

''' Provides utilities to work with saga configuration files and query strings.
'''

from copy import deepcopy

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
            # re-genereate query string
            new_query_string = ''
            for (key, value) in self._dictionary.iteritems():
                if value is None:
                    new_query_string += "&%s" % key
                else:
                    new_query_string += "&%s=%s" % (key, value)
            if new_query_string.startswith("&"):
                new_query_string = new_query_string[1:]
            self._query_string = new_query_string
            

    def as_query_string(self, subsection_name=None):
        ''' Return the configuration (or a subsection of it) as a URL 
            query string.
        '''
        self._re_eval()
        return deepcopy(self._query_string)

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
        return deepcopy(self._dictionary)

    def set_option(self, key, value=None):
        self._dictionary[key] = value
        self._re_eval

    def remove_option(self, key):
        del self._dictionary[key]
        self._re_eval

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
    cq.set_option('VERBOSE')
    print cq.as_query_string()
    cq.set_option('VERBOSE',False)
    print cq.as_query_string()
    cq.remove_option('VERBOSE')
    print cq.as_dict()

