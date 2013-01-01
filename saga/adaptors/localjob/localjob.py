# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, The SAGA Project"
__license__   = "MIT"

""" Local job adaptor implementation 
"""

import os, time, socket, signal, subprocess

from saga.utils.singleton import Singleton
from saga.utils.job.jobid import JobId
from saga.utils.which     import which

import saga.cpi.base
import saga.cpi.job

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

################################################################################
## the adaptor name                                                           ## 
##                                                                            ##
_ADAPTOR_NAME          = 'saga.adaptor.localjob'
_ADAPTOR_SCHEMAS       = ['fork', 'local']
_ADAPTOR_OPTIONS       = []

################################################################################
## the adaptor capabilites & supported attributes                             ##
##                                                                            ##
_ADAPTOR_CAPABILITES   = {
    'desc_attributes'  : [saga.job.EXECUTABLE,
                          saga.job.ARGUMENTS,
                          saga.job.ENVIRONMENT,
                          saga.job.WORKING_DIRECTORY,
                          saga.job.INPUT,
                          saga.job.OUTPUT,
                          saga.job.ERROR,
                          saga.job.SPMD_VARIATION,
                          saga.job.NUMBER_OF_PROCESSES],
    'job_attributes'   : [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    'metrics'          : [saga.job.STATE],
    'contexts'         : {'None' : """this adaptor works in the same security
                                      context as the application process itself."""
    }                  # {context type : how it is used}
}

################################################################################
## the adaptor documentation                                                  ##
##                                                                            ##
_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilites'      : _ADAPTOR_CAPABILITES,
    'description'      : """ 
        The local job adaptor. This adaptor uses subprocesses to run jobs on the 
        local machine.
        """,
    'details'          : """ 
        A more elaborate description....
        """,
    'schemas'          : {'fork'  :'desc', 
                          'local' :'same as fork'},
}

################################################################################
## the adaptor info is used to register the adaptor with SAGA                 ##
##                                                                            ##
_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [
        { 
        'type'         : 'saga.job.Service',
        'class'        : 'LocalJobService'
        }, 
        { 
        'type'         : 'saga.job.Job',
        'class'        : 'LocalJob'
        }
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.

    We only need one instance of this adaptor per process (actually per engine,
    but engine is a singleton, too...) -- the engine will though create new CPI
    implementation instances as needed (one per SAGA API object).
    """

    __metaclass__ = Singleton


    def __init__ (self) :

        saga.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # we only need to call gethostname once 
        self.hostname = socket.gethostname()


    def sanity_check (self) :
        pass



###############################################################################
#
class LocalJobService (saga.cpi.job.Service) :
    """ Implements saga.cpi.job.Service
    """
    def __init__ (self, api, adaptor) :
        """ Implements saga.cpi.job.Service.__init__
        """
        saga.cpi.Base.__init__ (self, api, adaptor, 'LocalJobService')


    @SYNC
    def init_instance (self, rm_url, session) :
        """ Service instance constructor
        """
        # check that the hostname is supported
        fqhn = Adaptor().hostname
        if rm_url.host != 'localhost' and rm_url.host != fqhn:
            msg = "Only 'localhost' and '%s' hostnames supported by this adaptor'" % (fqhn)
            raise saga.BadParameter._log (self._logger, msg)

        self._rm      = rm_url
        self._session = session

        # holds the jobs that were started via this instance
        self._jobs = dict() # {job_obj:id, ...}


    def _register_job(self, job_obj):
        """ Register job and job id with this service instance.  

            This is a convenience method and not part of the CPI.  A job can be
            registered repeatedly, in particular for delayed assignment of job
            IDs.
        """
        self._jobs[job_obj] = job_obj._id


    @SYNC
    def get_url (self) :
        """ Implements saga.cpi.job.Service.get_url()
        """
        return self._rm

    @SYNC
    def list(self):
        """ Implements saga.cpi.job.Service.list()
        """
        jobids = list()
        for (job_obj, job_id) in self._jobs.iteritems():
            if job_id is not None:
                jobids.append(job_id)
        return jobids

    @SYNC
    def create_job (self, jd) :
        """ Implements saga.cpi.job.Service.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITES['desc_attributes']:
                msg = "'JobDescription.%s' is not supported by this adaptor" % attribute
                raise saga.BadParameter._log (self._logger, msg)

        
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        state = { 'job_service'     : self, 
                  'job_description' : jd, 
                  'session'         : self._session }

        return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=state)


    @SYNC
    def get_job (self, jobid):
        """ Implements saga.cpi.job.Service.get_url()
        """
        if jobid not in self._jobs.values():
            msg = "Service instance doesn't know a Job with ID '%s'" % (jobid)
            raise saga.BadParameter._log (self._logger, msg)
        else:
            for (job_obj, job_id) in self._jobs.iteritems():
                if job_id == jobid:
                    return job_obj._api


    def container_run (self, jobs) :
        self._logger.debug("container run: %s"  %  str(jobs))
        # TODO: this is not optimized yet
        for job in jobs:
            job.run()


    def container_wait (self, jobs, mode, timeout) :
        self._logger.debug("container wait: %s"  %  str(jobs))
        # TODO: this is not optimized yet
        for job in jobs:
            job.wait()


    #def container_cancel (self, jobs) :
    #    self._logger.debug("container cancel: %s"  %  str(jobs))
    #    raise saga.NoSuccess("Not Implemented");


###############################################################################
#
class LocalJob (saga.cpi.job.Job) :
    """ Implements saga.cpi.job.Job
    """
    def __init__ (self, api, adaptor) :
        """ Implements saga.cpi.job.Job.__init__()
        """
        saga.cpi.Base.__init__ (self, api, adaptor, 'LocalJob')

    @SYNC
    def init_instance (self, job_info):
        """ Implements saga.cpi.job.Job.init_instance()
        """
        self._session         = job_info['session']
        self._jd              = job_info['job_description']
        self._parent_service  = job_info['job_service'] 

        # the _parent_service is responsible for job bulk operations -- which
        # for jobs only work for run()
        self._container       = self._parent_service
        self._method_type     = 'run'

        # initialize job attribute values
        self._id              = None
        self._state           = saga.job.NEW
        self._exit_code       = None
        self._exception       = None
        self._started         = None
        self._finished        = None
        self._execution_hosts = [Adaptor().hostname]
        
        # subprocess handle
        self._process         = None

        # register ourselves with the parent service
        # our job id is still None at this point
        self._parent_service._register_job(self)


    @SYNC
    def get_state(self):
        """ Implements saga.cpi.job.Job.get_state()
        """
        if self._state == saga.job.RUNNING:
            # only update if still running 
            self._exit_code = self._process.poll() 
            if self._exit_code is not None:
                if self._exit_code != 0:
                    self._exception = saga.NoSuccess ("non-zero exit code : %d" % self._exit_code)
                    self._state     = saga.job.FAILED
                else:
                    self._state = saga.job.DONE
                # TODO: this is not accurate -- job could have terminated 
                # before 'wait' was called
                self._finished = time.time() 
        return self._state

    @SYNC
    def wait(self, timeout):
        if self._process is None:
            msg = "Can't wait for job that has not been started"
            raise saga.IncorrectState._log (self._logger, msg)
        if timeout == -1:
            self._exit_code = self._process.wait()
        else:
            t_beginning = time.time()
            seconds_passed = 0
            while True:
                self._exit_code = self._process.poll()
                if self._exit_code is not None:
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
    def get_exit_code(self) :
        """ Implements saga.cpi.job.Job.get_exit_code()
        """        
        return self._exit_code

    @SYNC
    def get_created(self) :
        """ Implements saga.cpi.job.Job.get_started()
        """     
        # for local jobs started == created. for other adaptors 
        # this is not necessarily true   
        return self._started

    @SYNC
    def get_started(self) :
        """ Implements saga.cpi.job.Job.get_started()
        """        
        return self._started

    @SYNC
    def get_finished(self) :
        """ Implements saga.cpi.job.Job.get_finished()
        """        
        return self._finished
    
    @SYNC
    def get_execution_hosts(self) :
        """ Implements saga.cpi.job.Job.get_execution_hosts()
        """        
        return self._execution_hosts

    @SYNC
    def cancel(self, timeout):
        try:
            os.killpg(self._process.pid, signal.SIGTERM)
            self._exit_code = self._process.wait() # should return with the rc
            self._state = saga.job.CANCELED
        except OSError, ex:
            msg = "Couldn't cancel job %s: %s" % (self._id, ex)
            raise saga.IncorrectState._log (self._logger, msg)


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
            if jd.spmd_variation.lower() == "mpi":
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
                message = "SPMDVariation=MPI set, but can't find 'mpirun' executable."
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
            self._process = subprocess.Popen(cmdline, shell=True, 
                                             stderr=self._job_error, 
                                             stdout=self._job_output, 
                                             env=environment,
                                             cwd=cwd,
                                             preexec_fn=os.setsid)
            self._pid = self._process.pid

            jid = JobId()
            jid.native_id   = self._pid
            jid.backend_url = str(self._parent_service.get_url())

            self._state     = saga.job.RUNNING
            self._started   = time.time()
            self._id        = str(jid)

            self._parent_service._register_job(self)
            self._logger.debug("Starting process '%s' was successful." % cmdline) 

        except Exception, ex:
            msg = "Starting process: '%s' failed: %s" % (cmdline, str(ex))
            self._exception = saga.NoSuccess._log (self._logger, msg)
            self._state     = saga.job.FAILED
            raise self._exception


    @SYNC
    def re_raise(self):
        return self._exception

