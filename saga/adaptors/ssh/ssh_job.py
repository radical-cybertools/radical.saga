
""" SSH job adaptor implementation """

import saga.utils.which
import saga.utils.pty_shell

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job

import re
import os
import time

import ssh_wrapper

SYNC_CALL  = saga.adaptors.cpi.base.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.base.ASYNC_CALL


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.ssh_job"
_ADAPTOR_SCHEMAS       = ["fork", "ssh", "gsissh"]
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "jdes_attributes"  : [saga.job.EXECUTABLE,
                          saga.job.ARGUMENTS,
                          saga.job.ENVIRONMENT,
                          saga.job.INPUT,
                          saga.job.OUTPUT,
                          saga.job.ERROR],
    "job_attributes"   : [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    "metrics"          : [saga.job.STATE, 
                          saga.job.STATE_DETAIL],
    "contexts"         : {"ssh"      : "public/private keypair",
                          "x509"     : "X509 proxy for gsissh",
                          "userpass" : "username/password pair for simple ssh"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The SSH job adaptor. This adaptor uses the ssh command line tools to run
        remote jobs.
        """,
    "details"          : """ 
        A more elaborate description....

        Known Limitations:
        ------------------

          * number of system pty's are limited:  each job.service object bound
            to this adaptor will use 2 pairs of pty pipes.  Systems usually
            limit the number of available pty's to 1024 .. 4096.  Given that
            other process also use pty's , that gives a hard limit to the number
            of object instances which can be created concurrently.  Hitting the
            pty limit will cause the following error message (or similar)::

              NoSuccess: pty_allocation or process creation failed (ENOENT: no more ptys)

            This limitation comes from saga.utils.pty_process.  On Linux
            systems, the utilization of pty's can be monitored::

               echo "allocated pty's: `cat /proc/sys/kernel/pty/nr`"
               echo "available pty's: `cat /proc/sys/kernel/pty/max`"


          * number of ssh connections are limited: sshd's default configuration,
            which is in place on many systems, limits the number of concurrent
            ssh connections to 10 per user -- beyond that, connections are
            refused with the following error::

              NoSuccess: ssh_exchange_identification: Connection closed by remote host

            As the communication with the ssh channel is unbuffered, the
            dropping of the connection will likely cause this error message to
            be lost.  Instead, the adaptor will just see that the ssh connection
            disappeared, and will issue an error message similar to this one::

              NoSuccess: read from pty process failed (Could not read line - pty process died)

 
          * number of processes are limited: the creation of an job.service
            object will create one additional process on the local system, and
            two processes on the remote system (ssh daemon clone and a shell
            instance).  Each remote job will create three additional processes:
            two for the job instance itself (double fork), and an additional
            process which monitors the job for state changes etc.  Additional
            temporary processes may be needed as well.  

            While marked as 'obsolete' by POSIX, the `ulimit` command is
            available on many systems, and reports the number of processes
            available per user (`ulimit -u`)

            On hitting process limits, the job creation will fail with an error
            similar to either of these::
            
              NoSuccess: failed to run job (/bin/sh: fork: retry: Resource temporarily unavailable)
              NoSuccess: failed to run job -- backend error


          * Other system limits (memory, CPU, selinux, accounting etc.) apply as
            usual.

        """,
    "schemas"          : {"fork"   :"use /bin/sh to run jobs", 
                          "ssh"    :"use ssh to run remote jobs", 
                          "gsissh" :"use gsissh to run remote jobs"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
        { 
        "type"         : "saga.job.Service",
        "class"        : "SSHJobService"
        }, 
        { 
        "type"         : "saga.job.Job",
        "class"        : "SSHJob"
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


    # ----------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile ('^\[(.*)\]-\[(.*?)\]$')


    # ----------------------------------------------------------------
    #
    def sanity_check (self) :

        # FIXME: also check for gsissh

        pass


    def parse_id (self, id) :
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match (id)

        if  not match or len (match.groups()) != 2 :
            raise saga.BadParameter ("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group (2))


###############################################################################
#
class SSHJobService (saga.adaptors.cpi.job.Service) :
    """ Implements saga.adaptors.cpi.job.Service """

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        # FIXME: not sure if we should PURGE here -- that removes states which
        # might not be evaluated, yet.  Should we mark state evaluation
        # separately? 
        #   cmd_state () { touch $DIR/purgeable; ... }
        # When should that be done?

      # try :
      #   # if self.shell : self.shell.run_sync ("PURGE", iomode=None)
      #   # self._logger.trace ()
      #   # self._logger.breakpoint ()
      #     if self.shell : self.shell.run_sync ("QUIT" , iomode=None)
      # except :
      #     pass

        try :
            if self.shell : del (self.shell)
        except :
            pass


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, rm_url, session) :
        """ Service instance constructor """

        self.rm      = rm_url
        self.session = session

        self._open ()


    # ----------------------------------------------------------------
    #
    def _alive (self) :

        if  not self.shell or not self.shell.alive () :
            self._logger.info ("shell is dead - long live the shell")
            
            try :
                self._close ()  # for cleanup...
                self._open  ()

            except Exception :
                # did not work for good - give up
                raise saga.IncorrectState ("job service is not connected, can't reconnect")


    # ----------------------------------------------------------------
    #
    def _open (self) :

        # start the shell, find its prompt.  If that is up and running, we can
        # bootstrap our wrapper script, and then run jobs etc.
        self.shell = saga.utils.pty_shell.PTYShell (self.rm, self.session.contexts, self._logger)

        # -- now stage the shell wrapper script, and run it.  Once that is up
        # and running, we can requests job start / management operations via its
        # stdio.

        base = "$HOME/.saga/adaptors/ssh_job"

        ret, out, _ = self.shell.run_sync ("mkdir -p %s" % base)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to prepare base dir (%s)(%s)" % (ret, out))

        # FIXME: this is a race condition is multiple job services stage the
        # script at the same time.  We should make that atomic by
        #
        #   cat > .../wrapper.sh.$$ ... ; mv .../wrapper.sh.$$ .../wrapper.sh
        #
        # which should work nicely as long as compatible versions of the script
        # are staged.  Oh well...
        #
        # TODO: replace some constants in the script with values from config
        # files, such as 'timeout' or 'purge_on_quit' ...
        #
        self.shell.stage_to_file (src = ssh_wrapper._WRAPPER_SCRIPT, 
                                  tgt = "%s/wrapper.sh" % base)

        # we run the script.  In principle, we should set a new / different
        # prompt -- but, due to some strange and very unlikely coincidence, the
        # script has the same prompt as the previous shell... - go figure ;-)
        #
        # Note that we use 'exec' - so the script replaces the shell process.
        # Thus, when the script times out, the shell dies and the connection
        # drops -- that will free all associated resources, and allows for
        # a clean reconnect.
      # ret, out, _ = self.shell.run_sync ("exec sh %s/wrapper.sh" % base)
      
        # Well, actually, we do not use exec, as that does not give us good
        # feedback on failures (the shell just quits) -- so we replace it with
        # this poor-man's version...
      # ret, out, _ = self.shell.run_sync ("sh -c '(/bin/sh %s/wrapper.sh && kill -9 $PPID)' || false" % base)

        # well, this version logs an sh-x trace to wrapper.log, w/o disturbing
        # stdout/stderr...
        ret, out, _ = self.shell.run_sync ("exec sh -x %s/wrapper.sh 2>&1 | tee %s/wrapper.log | grep -v -e '^\\+'" % (base, base))

        # either way, we somehow ran the script, and just need to check if it
        # came up all right...
        if  ret != 0 :
            raise saga.NoSuccess ("failed to run wrapper (%s)(%s)" % (ret, out))


        self._logger.debug ("got cmd prompt (%s)(%s)" % (ret, out))


    # ----------------------------------------------------------------
    #
    def _close (self) :
        del (self.shell)
        self.shell = None


    # ----------------------------------------------------------------
    #
    #
    def _job_run (self, jd) :
        """ runs a job on the wrapper via pty, and returns the job id """


        exe = jd.executable
        arg = ""
        env = ""
        cwd = ""

        self._alive ()

        if jd.attribute_exists ("arguments") :
            for a in jd.arguments :
                arg += " %s" % a

        if jd.attribute_exists ("environment") :
            env = "/usr/bin/env"
            for e in jd.environment :
                env += " %s=%s"  %  (e, jd.environment[e])
            env += " "

        if jd.attribute_exists ("working_directory") :
            cwd = "cd %s && " % jd.working_directory

        cmd = "%s %s %s %s"  %  (env, cwd, exe, arg)

        ret, out, _ = self.shell.run_sync ("RUN %s" % cmd)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to run job '%s': (%s)(%s)" % (cmd, ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if  len (lines) < 2 :
            raise saga.NoSuccess ("failed to run job (%s)" % lines)

        if lines[-2] != "OK" :
            raise saga.NoSuccess ("failed to run job (%s)" % lines)

        job_id = "[%s]-[%s]" % (self.rm, lines[-1])

        self._logger.debug ("started job %s" % job_id)

        return job_id
        


    # ----------------------------------------------------------------
    #
    #
    def _job_get_state (self, id) :
        """ get the job state from the wrapper shell """

        rm, pid = self._adaptor.parse_id (id)

        ret, out, _ = self.shell.run_sync ("STATE %s\n" % pid)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to get job state for '%s': (%s)(%s)" \
                               % (id, ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if  len (lines) == 3 :
            del (lines[0])

        if  len (lines) != 2 :
            raise saga.NoSuccess ("failed to get job state for '%s': (%s)" % (id, lines))

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to get valid job state for '%s' (%s)" % (id, lines))

        return lines[1]
        


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job (self, jd) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES["jdes_attributes"]:
                msg = "'JobDescription.%s' is not supported by this adaptor" % attribute
                raise saga.BadParameter._log (self._logger, msg)

        
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = { "job_service"     : self, 
                          "job_description" : jd,
                          "job_schema"      : self.rm.schema }

        return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        return self.rm


  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def list(self):
  #     """ Implements saga.adaptors.cpi.job.Service.list()
  #     """
  #     jobids = list()
  #     for (job_obj, job_id) in self._jobs.iteritems():
  #         if job_id is not None:
  #             jobids.append(job_id)
  #     return jobids
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_job (self, jobid):
  #     """ Implements saga.adaptors.cpi.job.Service.get_url()
  #     """
  #     if jobid not in self._jobs.values():
  #         msg = "Service instance doesn't know a Job with ID '%s'" % (jobid)
  #         raise saga.BadParameter._log (self._logger, msg)
  #     else:
  #         for (job_obj, job_id) in self._jobs.iteritems():
  #             if job_id == jobid:
  #                 return job_obj.get_api ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # def container_run (self, jobs) :
  #     self._logger.debug("container run: %s"  %  str(jobs))
  #     # TODO: this is not optimized yet
  #     for job in jobs:
  #         job.run()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # def container_wait (self, jobs, mode, timeout) :
  #     self._logger.debug("container wait: %s"  %  str(jobs))
  #     # TODO: this is not optimized yet
  #     for job in jobs:
  #         job.wait()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # def container_cancel (self, jobs) :
  #     self._logger.debug("container cancel: %s"  %  str(jobs))
  #     raise saga.NoSuccess("Not Implemented");


###############################################################################
#
class SSHJob (saga.adaptors.cpi.job.Job) :
    """ Implements saga.adaptors.cpi.job.Job
    """
    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :
        """ Implements saga.adaptors.cpi.job.Job.__init__()
        """
        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, job_info):
        """ Implements saga.adaptors.cpi.job.Job.init_instance()
        """
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"] 

        # the js is responsible for job bulk operations -- which
        # for jobs only work for run()
        self._container       = self.js
        self._method_type     = "run"

        # initialize job attribute values
        self._id              = "default id"
        self._state           = saga.job.NEW
        self._exit_code       = None
        self._exception       = None
        self._started         = None
        self._finished        = None
        
        return self.get_api ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ Implements saga.adaptors.cpi.job.Job.get_state()
        """
        self._state = self.js._job_get_state (self._id)
        return self._state
  

  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def wait(self, timeout):
  #     pass
  #
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_id() """        
        return self._id
   
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_exit_code(self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_exit_code()
  #     """        
  #     return self._exit_code
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_created(self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_started()
  #     """     
  #     # for local jobs started == created. for other adaptors 
  #     # this is not necessarily true   
  #     return self._started
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_started(self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_started()
  #     """        
  #     return self._started
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_finished(self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_finished()
  #     """        
  #     return self._finished
  # 
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_execution_hosts(self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
  #     """        
  #     return self._execution_hosts
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def cancel(self, timeout):
  #     pass
  #
  #
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self): 
        """ Implements saga.adaptors.cpi.job.Job.run()
        """
        self._id = self.js._job_run (self.jd)


  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def re_raise(self):
  #     return self._exception


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

