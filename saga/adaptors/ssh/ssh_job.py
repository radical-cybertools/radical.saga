
""" SSH job adaptor implementation """

import saga.utils

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job

import time


SYNC_CALL  = saga.adaptors.cpi.base.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.base.ASYNC_CALL


# --------------------------------------------------------------------
# some private defs
#
_WRAPPER_SH = "https://raw.github.com/saga-project/saga-python/feature/sshjob/saga/adaptors/ssh/wrapper.sh"
_WRAPPER_SH = "/home/merzky/saga/saga-python/saga/adaptors/ssh/wrapper.sh"
_WRAPPER_SH = "/tmp/wrapper.sh"

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = 'saga.adaptor.ssh_job'
_ADAPTOR_SCHEMAS       = ['ssh', 'gsissh']
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    'jdes_attributes'  : [saga.job.EXECUTABLE,
                          saga.job.ARGUMENTS,
                          saga.job.ENVIRONMENT,
                          saga.job.INPUT,
                          saga.job.OUTPUT,
                          saga.job.ERROR],
    'job_attributes'   : [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    'metrics'          : [saga.job.STATE, 
                          saga.job.STATE_DETAIL],
    'contexts'         : {'ssh'      : "public/private keypair",
                          'x509'     : "X509 proxy for gsissh",
                          'userpass' : "username/password pair for simple ssh"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : """ 
        The SSH job adaptor. This adaptor uses the ssh command line tools to run
        remote jobs.
        """,
    'details'          : """ 
        A more elaborate description....
        """,
    'schemas'          : {'ssh'    :'use ssh to run a remote job', 
                          'gsissh' :'use gsissh to run a remote job'}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [
        { 
        'type'         : 'saga.job.Service',
        'class'        : 'SSHJobService'
        }, 
        { 
        'type'         : 'saga.job.Job',
        'class'        : 'SSHJob'
        }
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """


    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)


    def sanity_check (self) :

        # FIXME: also check for gsissh

        pass



###############################################################################
#
class SSHJobService (saga.adaptors.cpi.job.Service) :
    """ Implements saga.adaptors.cpi.job.Service """

    def __init__ (self, api, adaptor) :

        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)


    @SYNC_CALL
    def init_instance (self, adaptor_state, rm_url, session) :
        """ Service instance constructor """

        print 'x 1'

        self._rm      = rm_url
        self._session = session

        ssh_type = self._rm.schema.lower ()
        ssh_env  = "/usr/bin/env"
        ssh_exe  = ""
        ssh_args = "-tt"

        print 'x 2'

        if  ssh_type == 'ssh' :
            ssh_exe  = saga.utils.which ('ssh')
        elif ssh_type == 'gsissh' :
            ssh_exe = saga.utils.which ('gsissh')
        else :
            raise saga.BadParameter._log (self._logger, \
            	  "SSH Job adaptor can only handle ssh schema URLs, not %s" % self._rm.schema)


        print 'x 3'
        for context in self._session.contexts :
            if  context.type.lower () == "ssh" :
                if  ssh_type == 'ssh' :
                    if  context.attribute_exists ('user_id') :
                        ssh_args += " -l %s" % context.user_id
                    if  context.attribute_exists ('user_key') :
                        ssh_args += " -i %s" % context.user_key
            if  context.type.lower () == "gsissh" :
                if  ssh_type == 'gsissh' :
                    if  context.attribute_exists ('user_proxy') :
                        env += " X509_PROXY=%s" % context.user_proxy

        print 'x 4'
        ssh_args += " %s"       %  (self._rm.host)
        ssh_cmd   = "%s %s %s"  %  (ssh_env, ssh_exe, ssh_args)

        print "-------------------"
        self.pty  = saga.utils.pty_process (ssh_cmd, logfile='/tmp/t')

        find_prompt = True

        (n, match, lines) = self.pty.findline (['password\s*:\s*$', 
                                                'want to continue connecting', 
                                                'Last login'])
        while find_prompt :

            if n == 0 :
                self.pty.write ("secret\n")
                (n, match, lines) = self.pty.findline (['password\s*:\s*$', 
                                                        'want to continue connecting', 
                                                        'Last login'])
            elif n == 1 :
                self.pty.write ("yes\n")
                (n, match, lines) = self.pty.findline (['password\s*:\s*$', 
                                                        'want to continue connecting', 
                                                        'Last login'])
            elif n == 2 :
                self.pty.write ("export PS1='prompt-$?->'\n")
                (n, match) = self.pty.find (['^prompt-[\d+]->$'], 10.0)
                find_prompt = False
        
        # we have a prompt on the remote system -- now fetch the shell wrapper
        # script, and run it.  Once that is up and running, we can requests job
        # start / management operations via its stdio

      # self.pty.write ("mkdir   -p       $HOME/.saga/adaptors/ssh_job/ && " + \
      #                 "wget    -q %s -O $HOME/.saga/adaptors/ssh_job/wrapper.sh\n" % _WRAPPER_SH)
        self.pty.write ("mkdir   -p $HOME/.saga/adaptors/ssh_job/ && " + \
                        "/bin/cp %s $HOME/.saga/adaptors/ssh_job/wrapper.sh\n" % _WRAPPER_SH)
        
        _, match = self.pty.find (['^prompt-[\d+]->$'], 10.0)

        if not match[-4:] == "-0->" :
            raise saga.NoSuccess ("failed to retrieve wrapper: %s" % match[:-10])


        self.pty.write ("/bin/sh $HOME/.saga/adaptors/ssh_job/wrapper.sh\n")

        _, match = self.pty.find  (['^CMD\s*'], 10.0)


    def _run_job (self, jd) :
        """ runs a job on the wrapper via pty, and returns the job id """

        exe = jd.executable
        arg = ""
        env = ""
        cwd = ""

        if jd.attribute_exists ('arguments') :
            for a in jd.arguments :
                arg += " %s" % a

        if jd.attribute_exists ('environment') :
            env = "/usr/bin/env"
            for e in jd.environment :
                env += " %s=%s"  %  (e, jd.environment[e])
            env += " "

        if jd.attribute_exists ('working_directory') :
            cwd = "cd %s && " % jd.working_directory

        cmd = "%s %s %s %s"  %  (env, cwd, exe, arg)

        self.pty.write ("RUN %s\n" % cmd)
        _, match = self.pty.find (['CMD'], 10.0)


        splitlines = match.split ('\n')
        lines = []

        for line in splitlines :
            if len (line) :
                lines.append (line)
        self._logger.debug (str(lines))

        if len (lines) < 3 :
            self._logger.warn ("Cannot interpret response from wrapper.sh (%s)" % str (lines))
            raise saga.NoSuccess ("failed to run job")
        elif lines[1] != 'OK' :
            self._logger.warn ("Did not find 'OK' from wrapper.sh (%s)" % str (lines))
            raise saga.NoSuccess ("failed to run job")

        job_id = "[%s]-[%s]" % (self._rm, lines[2])

        self._logger.debug ("started job %s" % job_id)

        return job_id
        


    @SYNC_CALL
    def create_job (self, jd) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES['jdes_attributes']:
                msg = "'JobDescription.%s' is not supported by this adaptor" % attribute
                raise saga.BadParameter._log (self._logger, msg)

        
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = { 'job_service'     : self, 
                          'job_description' : jd,
                          'job_schema'      : self._rm.schema }

        return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        return self._rm


    @SYNC_CALL
    def list(self):
        """ Implements saga.adaptors.cpi.job.Service.list()
        """
        jobids = list()
        for (job_obj, job_id) in self._jobs.iteritems():
            if job_id is not None:
                jobids.append(job_id)
        return jobids


    @SYNC_CALL
    def get_job (self, jobid):
        """ Implements saga.adaptors.cpi.job.Service.get_url()
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
class SSHJob (saga.adaptors.cpi.job.Job) :
    """ Implements saga.adaptors.cpi.job.Job
    """
    def __init__ (self, api, adaptor) :
        """ Implements saga.adaptors.cpi.job.Job.__init__()
        """
        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)

    @SYNC_CALL
    def init_instance (self, job_info):
        """ Implements saga.adaptors.cpi.job.Job.init_instance()
        """
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
        
        # subprocess handle
        self._process         = None

        return self._api


    @SYNC_CALL
    def get_state(self):
        """ Implements saga.adaptors.cpi.job.Job.get_state()
        """
        return self._state

    @SYNC_CALL
    def wait(self, timeout):
        pass

    @SYNC_CALL
    def get_id (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_id()
        """        
        return self._id

    @SYNC_CALL
    def get_exit_code(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_exit_code()
        """        
        return self._exit_code

    @SYNC_CALL
    def get_created(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_started()
        """     
        # for local jobs started == created. for other adaptors 
        # this is not necessarily true   
        return self._started

    @SYNC_CALL
    def get_started(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_started()
        """        
        return self._started

    @SYNC_CALL
    def get_finished(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_finished()
        """        
        return self._finished
    
    @SYNC_CALL
    def get_execution_hosts(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """        
        return self._execution_hosts

    @SYNC_CALL
    def cancel(self, timeout):
        pass


    @SYNC_CALL
    def run(self): 
        """ Implements saga.adaptors.cpi.job.Job.run()
        """
        self._id = self._parent_service._run_job (self._jd)
        print self._id


    @SYNC_CALL
    def re_raise(self):
        return self._exception


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

