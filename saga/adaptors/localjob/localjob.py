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

    def __init__ (self, api) :
        """ Creates a new local job service.
        """
        saga.cpi.Base.__init__ (self, api, _adaptor_name)

    @SYNC
    def init_instance (self, rm_url, session) :
        """ Service instance constructor
        """
        if rm_url.host != 'localhost' and rm_url.host != socket.gethostname():
            raise saga.BadParameter(message='ss') 

        self._rm      = rm_url
        self._session = session
        _SharedData().dict['services'][self._rm] = self




    @SYNC
    def get_url (self) :

        return self._rm


    @SYNC
    def create_job (self, jd) :
        # print jd._attributes_dump ()
        j = saga.job.Job._create_from_adaptor ("my_id", self._session, "fork", _adaptor_name)
        return j


######################################################################
#
# job adaptor class
#
class LocalJob (saga.cpi.job.Job) :

    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, _adaptor_name)
        # print "local job adaptor init";

    @SYNC
    def init_instance (self, id, session) :
        # print "local job adaptor instance init sync %s" % id
        self._id      = id
        self._session = session
        _SharedData().dict['jobs'][self._id] = self
        #_adaptor_state.dump ()


    @SYNC
    def get_id (self) :
        # print "sync get_id"
        return self._id

    @ASYNC
    def get_id_async (self, ttype) :
        # print "async get_id"
        t = saga.task.Task ()
        return t


