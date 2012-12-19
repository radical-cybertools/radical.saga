# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, The SAGA Project"
__license__   = "MIT"

""" Local job adaptor implementation 
"""

import os, time, socket, subprocess

from saga.utils.singleton import Singleton
from saga.utils.job.jobid import JobId
from saga.utils.which import which

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
            if attribute not in _adaptor_capabilities['jd_attributes']:
                raise saga.BadParameter('JobDescription.%s is not supported by this adaptor' % attribute)
        
        new_job = saga.job.Job._create_from_adaptor (jd, self._session, 
                                                     self._rm.scheme, 
                                                     _adaptor_name)
        return new_job


###############################################################################
#
class LocalJob (saga.cpi.job.Job) :
    """ Implements saga.cpi.job.Job
    """
    def __init__ (self, api) :
        """ Implements saga.cpi.job.Job.__init__()
        """
        saga.cpi.Base.__init__ (self, api, _adaptor_name)

    @SYNC
    def init_instance (self, job_description, session):
        """ Implements saga.cpi.job.Job.init_instance()
        """
        self._session    = session
        self._jd         = job_description

        self._id         = None
        self._state      = saga.job.NEW
        self._returncode = None
        
        # The subprocess handle
        self._process    = None

        _SharedData().dict['jobs'][self] = "hi"

    @SYNC
    def get_state(self):
        """ Implements saga.cpi.job.Job.get_state()
        """
        if self._state == saga.job.RUNNING:
            # only update if still running 
            self._returncode = self._process.poll() 
            if self._returncode is not None:
                if self._returncode != 0:
                    self._state = saga.job.FAILED
                else:
                    self._state = saga.job.DONE
        return self._state

    @SYNC
    def wait(self, timeout):
        if self.get_state() != saga.job.RUNNING:
            raise SagaException('not in running state')

        if timeout == -1:
            self._returncode = self._process.wait()
        else:
            t_beginning = time.time()
            seconds_passed = 0
            while True:
                self._returncode = self._process.poll()
                if self._returncode is not None:
                    break
                seconds_passed = time.time() - t_beginning
                if timeout and seconds_passed > timeout:
                    break
                time.sleep(0.5)

    @SYNC
    def get_id (self) :
        """ Implements saga.cpi.job.Job.get_id()
        """        
        return self._id

    @SYNC
    def run(self): 
        """ Implements saga.cpi.job.Job.run()
        """
        # lots of attribute checking and such 
        executable  = self._jd.executable
        arguments   = self._jd.arguments
        environment = self._jd.environment
        cwd         = self._jd.working_directory
        
        # check if we want to write stdout to a file
        if self._jd.output is not None:
            if os.path.isabs(self._jd.output):
                self._job_output = open(jd.output,"w")  
            else:
                if cwd is not None:
                    self._job_output = open(os.path.join(cwd, self._jd.output),"w")
                else:
                    self._job_output = open(self._jd.output,"w")  
        else:
            self._job_output = None 

        # check if we want to write stderr to a file
        if self._jd.error is not None:
            if os.path.isabs(self._jd.error):
                self._job_error = open(self._jd.error,"w")  
            else:
                if cwd is not None:
                    self._job_error = open(os.path.join(self.cwd, self._jd.error),"w")
                else:
                    self._job_error = open(self._jd.error,"w") 
        else:
            self._job_error = None

        # check if we want to execute via mpirun
        if self._jd.spmd_variation is not None:
            if jd.spmd_variation == "MPI":
                if self._jd.number_of_processes is not None:
                    self.number_of_processes = self._jd.number_of_processes
                    use_mpirun = True
                self._logger.info("SPMDVariation=%s requested. Job will execute via 'mpirun -np %d'." % (self._jd.spmd_variation, self.number_of_processes))
            else:
                self._logger.warning("SPMDVariation=%s: unsupported SPMD variation. Ignoring." % self._jd.spmd_variation)
        else:
            use_mpirun = False

        # check if executable exists.
        if which(executable) == None:
            message = "Executable '%s' doesn't exist or is not in the path" % executable
            self._logger.error(message)        
            raise saga.BadParameter(message)

        # check if you can do mpirun
        if use_mpirun is True:
            which('mpirun')
            if mpirun == None:
                message = "Can't find 'mpirun' in the path."
                self._logger.error(message)        
                raise saga.BadParameter(message) 
            else:
                cmdline = '%s -np %d %s' % (mpirun, self.number_of_processes, str(self.executable))
        else:
            cmdline = str(executable)
        args = ""
        if arguments is not None:
            for arg in arguments:
                cmdline += " %s" % arg 

        # now we can finally try to run the job via subprocess
        try:
            self._logger.debug("Trying to execute: %s" % cmdline) 
            self._process = subprocess.Popen(cmdline, shell=True, 
                                             stderr=self._job_error, 
                                             stdout=self._job_output, 
                                             env=environment,
                                             cwd=cwd)
            self._pid = self._process.pid
            self._state = saga.job.RUNNING

            jid = JobId()
            jid.native_id = self._pid
            jid.backend_url = 'fff'
            self._id = str(jid)

        except Exception, ex:
            raise saga.NoSuccess(str(ex))

