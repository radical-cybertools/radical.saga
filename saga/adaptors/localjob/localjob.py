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
from saga.utils.which import which

import saga.cpi.base
import saga.cpi.job

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

###############################################################################
# Adaptor capabilites and documentaton.
#
# - name
# - description
# - detailed description
# - supported schemas (& what they do)
# - supported config options -- Configurable
# - supported job descrption attributes
# - supported metrics
# - supported contexts

# the adaptor name
_adaptor_name   = 'saga.adaptor.LocalJob'

_adaptor_config_options = [
    { 
    'category'      : _adaptor_name,
    'name'          : 'foo', 
    'type'          : str, 
    'default'       : 'bar', 
    'valid_options' : None,
    'documentation' : 'dummy config option for unit test.',
    'env_variable'  : None
    }
]

_adaptor_capabilities = {
    'jd_attributes' : [saga.job.EXECUTABLE,
                       saga.job.ARGUMENTS,
                       saga.job.ENVIRONMENT,
                       saga.job.WORKING_DIRECTORY,
                       saga.job.INPUT,
                       saga.job.OUTPUT,
                       saga.job.ERROR,
                       saga.job.SPMD_VARIATIONS,
                       saga.job.NUMBER_OF_PROCESSES],
    'metrics'       : [saga.job.STATE],
    'contexts'      : {}  # {context type : how it is used}
}

_adaptor_doc = {
    'name'        : '',
    'description' : '',
    'details'     : '',
    'schemas'     : {'fork':'desc', 'local':'same as fork'},
    'cfg_options' : _adaptor_config_options,
    'capabilites' : _adaptor_capabilites,

                }


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
class _SharedData(object) :
    """ This class is shared between all adaptor instances. 
        We use it to share information and data.
    """
    __metaclass__ = Singleton

    def __init__ (self) :
        self._services = dict()

    def add_service_instance(self, service_obj):
        """ Adds a new service object to the list of known
            services.
        """
        self._services[service_obj] = {'url':str(service_obj.get_url()), 
                                       'known_jobs':{'1':'<obj>'}}

    def get_known_job_ids(self, service_obj):
        """ Returns a list of job ids that are known
            by a particular serivce.
        """
        job_ids = list()
        for (serviceobj, data) in self._services.iteritems():
            for (jobid, jobobj) in data['known_jobs'].iteritems():
                job_ids.append(jobid)

    def _dump(self):
        """ Dumps the content of _SharedData to stdout. This can
            be useful for debugging.
        """

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
        # check that the hostname is supported
        fqhn = socket.gethostname()
        if rm_url.host != 'localhost' and rm_url.host != fqhn:
            message = "Only 'localhost' and '%s' hostnames supported by this adaptor'" % (fqhn)
            self._logger.warning(message)
            raise saga.BadParameter(message=message) 

        self._rm      = rm_url
        self._session = session

        # holds the jobs that were started via this instance
        self._jobs = dict() # {job_obj:id, ...}

        # add this service to the list of known services which is 
        # accessible from all local service/job instances. 
        # entries are in the form: fork://localhost -> instance 
        _SharedData().add_service_instance(self)

    def _update_jobid(self, job_obj, job_id):
        """ Update the job id for a job object registered 
            with this service instance.

            This is a convenince method and not part of the CPI.
        """
        self._jobs[job_obj] = job_id 


    @SYNC
    def get_url (self) :
        """ Implements saga.cpi.job.Serivce.get_url()
        """
        return self._rm

    @SYNC
    def list(self):
        """ Implements saga.cpi.job.Serivce.list()
        """
        jobids = list()
        for (job_obj, job_id) in self._jobs.iteritems():
            if job_id is not None:
                jobids.append(job_id)
        return jobids

    @SYNC
    def create_job (self, jd) :
        """ Implements saga.cpi.job.Serivce.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _adaptor_capabilities['jd_attributes']:
                raise saga.BadParameter('JobDescription.%s is not supported by this adaptor' % attribute)
        
        # create and return the new job
        job_info = { 'job_service'     : self, 
                     'job_description' : jd, 
                     'session'         : self._session,
                     'container'       : self }

        job  = saga.job.Job._create_from_adaptor(job_info,
                                                 self._rm.scheme, 
                                                 _adaptor_name)
        return job

    @SYNC
    def get_job (self, jobid):
        """ Implements saga.cpi.job.Service.get_url()
        """
        if jobid not in self._jobs.values():
            message = "This Service instance doesn't know a Job with ID '%s'" % (jobid)
            self._logger.error(message)
            raise saga.BadParameter(message=message) 
        else:
            for (job_obj, job_id) in self._jobs.iteritems():
                if job_id == jobid:
                    return job_obj._api


    def container_run (self, result_queue, jobs) :
        self._logger.debug("container run: %s"  %  str(jobs))
        #raise saga.NoSuccess("Ole is lazy...")


    def container_wait (self, result_queue, jobs, mode) :
        self._logger.debug("container wait: %s"  %  str(jobs))

        for ajob in jobs:
            ajob.wait()


        #raise saga.NoSuccess("Ole is lazy...")


    def container_cancel (self, result_queue, jobs) :
        self._logger.debug("container cancel: %s"  %  str(jobs))
        raise saga.NoSuccess("Ole is lazy...");


    #def container_get_states (self, result_queue, jobs) :
    #    self._logger.debug("container get_states: %s"  %  str(jobs))
    #    raise saga.NoSuccess("Ole is lazy...");


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
    def init_instance (self, job_info):
        """ Implements saga.cpi.job.Job.init_instance()
        """
        self._session        = job_info['session']
        self._jd             = job_info['job_description']
        self._parent_service = job_info['job_service'] 

        if 'container' in job_info :
            self._container = job_info['container']
        else :
            self._container = None

        self._id         = None
        self._state      = saga.job.NEW
        self._returncode = None
        
        # The subprocess handle
        self._process    = None

        # register ourselves with the parent service
        # our job id is still None at this point
        self._parent_service._update_jobid(self, None)


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
        if self._process is None:
            message = "Can't wait for job. Job has not been started"
            self._logger.warning(message)
            raise saga.IncorrectState(message)
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
    def get_exit_code(self) :
        """ Implements saga.cpi.job.Job.get_exit_code()
        """        
        return self._returncode

    @SYNC
    def cancel(self, timeout):
        try:
            os.killpg(self._process.pid, signal.SIGTERM)
            self._returncode = self._process.wait() # should return with the rc
            self._state = saga.job.CANCELED
        except OSError, ex:
            raise saga.IncorrectState("Couldn't cancel job %s: %s" % (self._id, ex))


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
            self._logger.debug("Trying to execute: %s" % cmdline) 
            self._process = subprocess.Popen(cmdline, shell=True, 
                                             stderr=self._job_error, 
                                             stdout=self._job_output, 
                                             env=environment,
                                             cwd=cwd,
                                             preexec_fn=os.setsid)
            self._pid = self._process.pid
            self._state = saga.job.RUNNING

            jid = JobId()
            jid.native_id = self._pid
            jid.backend_url = str(self._parent_service.get_url())
            self._id = str(jid)
            self._parent_service._update_jobid(self, self._id)

        except Exception, ex:
            raise saga.NoSuccess(str(ex))

