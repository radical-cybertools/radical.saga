# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, The SAGA Project"
__license__   = "MIT"

""" Local job adaptor implementation 
"""

import socket

from saga.utils import Singleton

import saga.cpi.base
import saga.cpi.job

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

###############################################################################
#
class _SharedData(object) :
    """ This class is shared between all adaptor instances. 
        We use it to share information and data.
    """
    __metaclass__ = Singleton

    def __init__ (self) :
        self.dict = {}
        self.dict['services'] = {}
        self.dict['jobs']     = {}


_adaptor_name   = 'saga.adaptor.LocalJob'
_adaptor_info   = [{ 'name'    : _adaptor_name,
                     'type'    : 'saga.job.Service',
                     'class'   : 'LocalJobService',
                     'schemas' : ['fork', 'local']
                   }, 
                   { 'name'    : _adaptor_name,
                     'type'    : 'saga.job.Job',
                     'class'   : 'LocalJob',
                     'schemas' : ['fork', 'local']
                   }]
_adaptor_capabilities = {'jd_attributes' : [saga.job.EXECUTABLE,
                                            saga.job.ARGUMENTS,
                                            saga.job.ENVIRONMENT,
                                            saga.job.WORKING_DIRECTORY,
                                            saga.job.INPUT,
                                            saga.job.OUTPUT,
                                            saga.job.ERROR],
                         'metrics'       : [saga.job.STATE]
                        }

###############################################################################
#
def register () :
    """ Adaptor registration function. The engine calls this during startup. 

        We usually do sanity checks here and throw and exception if we think
        the adaptor won't work in a given context. In that case, the engine
        won't add it to it's internal list of adaptors. If everything is ok,
        we return the adaptor info.
    """

    # perform some sanity checks, like check if dependencies are met
    return _adaptor_info


###############################################################################
#
class LocalJobService (saga.cpi.job.Service) :
    """ Implements saga.cpi.job.Serivce
    """
    def __init__ (self, api) :
        """ Implements saga.cpi.job.Serivce.__init__
        """
        saga.cpi.Base.__init__ (self, api, _adaptor_name)

    @SYNC
    def init_instance (self, rm_url, session) :
        """ Service instance constructor
        """
        fqhn = socket.gethostname()
        if rm_url.host != 'localhost' and rm_url.host != fqhn:
            message = "Only 'localhost' and '%s' hostnames supported byt this adaptor'" % (fqhn)
            self._logger.warning(message)
            raise saga.BadParameter(message=message) 

        self._rm      = rm_url
        self._session = session

        _SharedData().dict['services'][self._rm] = self

    @SYNC
    def get_url (self) :
        """ Implements saga.cpi.job.Serivce.get_url()
        """
        return self._rm

    @SYNC
    def create_job (self, jd) :
        """ Implements saga.cpi.job.Serivce.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _adaptor_capabilities:
                raise saga.BadParameter('JobDescription.%s is not supported by this adaptor' % attribute)
        
        new_job = saga.job.Job._create_from_adaptor (self._session, "fork", _adaptor_name)
        return new_job


######################################################################
#
# job adaptor class
#
class LocalJob (saga.cpi.job.Job) :

    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, _adaptor_name)
        # print "local job adaptor init";

    @SYNC
    def init_instance (self, session) :
        self._id      = None
        self._session = session
        _SharedData().dict['jobs'][self] = "hi"
        #_adaptor_state.dump ()


    @SYNC
    def get_id (self) :
        # print "sync get_id"
        return self._id

    @ASYNC
    def get_id_async (self, ttype) :
        t = saga.task.Task ()
        return t


