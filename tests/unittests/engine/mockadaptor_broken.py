
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Unit test mock adaptor for saga.engine.engine.py
"""

from   saga.utils.singleton import Singleton

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job


_ADAPTOR_NAME        = 'saga.adaptor.mock'
_ADAPTOR_SCHEMAS     = ['mock']
_ADAPTOR_DOC         = {}
_ADAPTOR_CAPABILITES = {}
_ADAPTOR_OPTIONS     = [] 
_ADAPTOR_INFO        = {
    'name'           : _ADAPTOR_NAME,
    'version'        : '1.0',
    'schemas'        : _ADAPTOR_SCHEMAS,
    'cpis'           : [{ 
        'type'       : 'saga.job.Job',
        'class'      : 'MockJob'
        }
    ]
}


class Adaptor (saga.adaptors.cpi.base.AdaptorBase):

    __metaclass__ = Singleton

    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS) 


    def sanity_check (self) :
        """ We do sanity checks here and throw and exception if we think
            the adaptor won't work in a given environment. In that case, the
            engine won't add it to it's internal list of adaptors. 
        """
    
        raise Exception("CRAP! Well, actually this is supposed to happen... ;-)")



class MockJob (saga.adaptors.cpi.job.Job) :

    def __init__ (self, api, adaptor) :

        saga.adaptors.cpi.Base.__init__ (self, api, adaptor)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

