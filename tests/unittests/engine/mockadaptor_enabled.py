# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Unit test mock adaptor for saga.engine.engine.py
"""

import saga.cpi.base
import saga.cpi.job

_adaptor_info = [{'name'    : 'saga.adaptor.mock',
                  'type'    : 'saga.job.Job',
                  'class'   : 'MockJob',
                  'schemas' : ['mock']
                 }]

_adaptor_config_options = [
    { 
    'category'      : 'saga.adaptor.mock',
    'name'          : 'foo', 
    'type'          : str, 
    'default'       : 'bar', 
    'valid_options' : None,
    'documentation' : 'Mock option.',
    'env_variable'  : None
    }
]

def register():
    return _adaptor_info

class MockJob(saga.cpi.job.Job):
    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, adaptor_name='saga.adaptor.mock',
                                      config_options=_adaptor_config_options)

