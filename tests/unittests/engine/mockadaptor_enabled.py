
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit test mock adaptor for saga.engine.engine.py
"""

from   radical.utils import Singleton
import radical.saga.adaptors.cpi.base
import radical.saga.adaptors.cpi.job

import radical.utils as ru


_ADAPTOR_NAME        = 'radical.saga.adaptors.mock'
_ADAPTOR_SCHEMAS     = ['mock']
_ADAPTOR_DOC         = {}
_ADAPTOR_CAPABILITES = {}

_ADAPTOR_INFO        = {
    'name'           : _ADAPTOR_NAME,
    'version'        : '1.0',
    'schemas'        : _ADAPTOR_SCHEMAS,
    'cpis'           : [{ 
        'type'       : 'radical.saga.job.Job',
        'class'      : 'MockJob'
        }
    ]
}


class Adaptor (radical.saga.adaptors.base.Base):

    __metaclass__ = Singleton

    def __init__ (self) :

        radical.saga.adaptors.base.Base.__init__ (self, _ADAPTOR_INFO) 


    def sanity_check (self) :
        pass


class MockJob(radical.saga.adaptors.cpi.job.Job):

    def __init__ (self, api, adaptor) :

        self._cpi_base = super(MockJob, self)
        self._cpi_base.__init__(api, adaptor)




