
""" shell based job adaptor implementation """

import saga.utils.which
import saga.utils.pty_shell

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job

from   saga.job.constants import *

import re
import os
import time
import threading

import shell_wrapper

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.shell_job"
_ADAPTOR_SCHEMAS       = ["fork", "local", "ssh", "gsissh"]
_ADAPTOR_OPTIONS       = [
    { 
    'category'         : 'saga.adaptor.shell_job',
    'name'             : 'enable_notifications', 
    'type'             : bool, 
    'default'          : False,
    'valid_options'    : [True, False],
    'documentation'    : '''Enable support for job state notifications.  Note that
                          enabling this option will create a local thread, a remote 
                          shell process, and an additional network connection.
                          In particular for ssh/gsissh where the number of
                          concurrent connections is limited to 10, this
                          effectively halfs the number of available job service
                          instances per remote host.''',
    'env_variable'     : None
    }
]

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
                          "userpass" : "username/password pair for ssh"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The Shell job adaptor. This adaptor uses the sh command line tools (sh,
        ssh, gsissh) to run remote jobs.
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

          * number of files are limited, as is disk space: the job.service will
            
            keep job state on the remote disk, in ``$HOME/.saga/adaptors/shell_job/``.
            Quota limitations may limit the number of files created there,
            and/or the total size of that directory.  

            On quota or disk space limits, you may see error messages similar to
            the following ones::
            
              NoSuccess: read from pty process failed ([Errno 5] Quota exceeded)
              NoSuccess: read from pty process failed ([Errno 5] Input/output error)
              NoSuccess: find from pty process [Thread-5] failed (Could not read - pty process died)
              


          * Other system limits (memory, CPU, selinux, accounting etc.) apply as
            usual.

          
          * thread safety: it is safe to create multiple :class:`job.Service`
            instances to the same target host at a time -- they should not
            interfere with each other, but ``list()`` will list jobs created by
            either instance (if those use the same target host user account).

            It is **not** safe to use the *same* :class:`job.Service` instance
            from multiple threads concurrently -- the communication on the I/O
            channel will likely get screwed up.  This limitation may be removed
            in future versions of the adaptor.  Non-concurrent (i.e. serialized)
            use should work as expected though.

        """,
    "schemas"          : {"fork"   :"use /bin/sh to run jobs", 
                          "local"  :"alias for fork://", 
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
        "class"        : "ShellJobService"
        }, 
        { 
        "type"         : "saga.job.Job",
        "class"        : "ShellJob"
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
        self.opts  = self.get_config ()

        self.notifications = self.opts['enable_notifications'].get_value ()

        self._logger.debug ('threading id: %s' % threading.current_thread ().name)


    # ----------------------------------------------------------------
    #
    def sanity_check (self) :

        # FIXME: also check for gsissh

        pass


    # ----------------------------------------------------------------
    #
    def parse_id (self, id) :
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match (id)

        if  not match or len (match.groups()) != 2 :
            raise saga.BadParameter ("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group (2))


    # ----------------------------------------------------------------
    #
    def string_to_state (self, state_str) :

        state_str = state_str.strip ()

        if state_str.lower () == 'new'       : return saga.job.NEW
        if state_str.lower () == 'running'   : return saga.job.RUNNING
        if state_str.lower () == 'suspended' : return saga.job.SUSPENDED
        if state_str.lower () == 'done'      : return saga.job.DONE
        if state_str.lower () == 'failed'    : return saga.job.FAILED
        if state_str.lower () == 'canceled'  : return saga.job.CANCELED

        return saga.job.UNKNOWN


###############################################################################
#
class ShellJobService (saga.adaptors.cpi.job.Service) :
    """ Implements saga.adaptors.cpi.job.Service """

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (ShellJobService, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        # FIXME: not sure if we should PURGE here -- that removes states which
        # might not be evaluated, yet.  Should we mark state evaluation
        # separately? 
        #   cmd_state () { touch $DIR/purgeable; ... }
        # When should that be done?
        self._logger.error ("adaptor dying... %s" % self.njobs)
        self._logger.trace ()
    
        #     try :
        #       # if self.shell : self.shell.run_sync ("PURGE", iomode=None)
        #         if self.shell : self.shell.run_sync ("QUIT" , iomode=None)
        #     except :
        #         pass

        self.finalize (kill_shell=True)
    




    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, rm_url, session) :
        """ Service instance constructor """

        self.rm      = rm_url
        self.session = session
        self.njobs   = 0

        self.shell = saga.utils.pty_shell.PTYShell (self.rm, self.session.contexts, 
                                                    self._logger)

        self.shell.set_initialize_hook (self.initialize)
        self.shell.set_finalize_hook   (self.finalize)

        self.initialize ()

    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # start the shell, find its prompt.  If that is up and running, we can
        # bootstrap our wrapper script, and then run jobs etc.

        # -- now stage the shell wrapper script, and run it.  Once that is up
        # and running, we can requests job start / management operations via its
        # stdio.

        base = "$HOME/.saga/adaptors/shell_job"

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
        self.shell.stage_to_file (src = shell_wrapper._WRAPPER_SCRIPT, 
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
        ret, out, _ = self.shell.run_sync ("/bin/sh -c '/bin/sh %s/wrapper.sh $$ && kill -9 $PPID' || false" \
                                        % (base))

        # either way, we somehow ran the script, and just need to check if it
        # came up all right...
        if  ret != 0 :
            raise saga.NoSuccess ("failed to run wrapper (%s)(%s)" % (ret, out))

        self._logger.debug ("got cmd prompt (%s)(%s)" % (ret, out.strip ()))


    # ----------------------------------------------------------------
    #
    def finalize (self, kill_shell = False) :

        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)


    
    # ----------------------------------------------------------------
    #
    #
    def _jd2cmd (self, jd) :

        exe = jd.executable
        arg = ""
        env = ""
        cwd = ""

        if jd.attribute_exists ("arguments") :
            for a in jd.arguments :
                arg += ' ' + a

        if jd.attribute_exists ("environment") :
            for e in jd.environment :
                env += "export %s=%s; "  %  (e, jd.environment[e])

        if jd.attribute_exists ("working_directory") :
            cwd = "cd %s && " % jd.working_directory

        cmd = "%s%s %s %s"  %  (env, cwd, exe, arg)

        return cmd

    # ----------------------------------------------------------------
    #
    #
    def _job_run (self, jd) :
        """ runs a job on the wrapper via pty, and returns the job id """

        cmd = self._jd2cmd (jd)

        ret, out, _ = self.shell.run_sync ("RUN %s" % cmd)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to run job '%s': (%s)(%s)" % (cmd, ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if  len (lines) < 2 :
            raise saga.NoSuccess ("failed to run job (%s)" % lines)

        if lines[-2] != "OK" :
            raise saga.NoSuccess ("failed to run job (%s)" % lines)

        # FIXME: verify format of returned pid (\d+)!
        pid    = lines[-1].strip ()
        job_id = "[%s]-[%s]" % (self.rm, pid)

        self._logger.debug ("started job %s" % job_id)

        self.njobs += 1

        return job_id
        


    # ----------------------------------------------------------------
    #
    #
    def _job_get_state (self, id) :
        """ get the job state from the wrapper shell """

        rm, pid     = self._adaptor.parse_id (id)
        ret, out, _ = self.shell.run_sync ("STATE %s\n" % pid)

        if  ret != 0 :
            raise saga.NoSuccess ("failed to get job state for '%s': (%s)(%s)" \
                               % (id, ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if  len (lines) == 3 :
            # shell did not manage to do 'stty -echo'?
            del (lines[0])

        if  len (lines) != 2 :
            raise saga.NoSuccess ("failed to get job state for '%s': (%s)" % (id, lines))

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to get valid job state for '%s' (%s)" % (id, lines))

        return self._adaptor.string_to_state (lines[1])
        

    # ----------------------------------------------------------------
    #
    # TODO: this should also fetch the (final) state, to safe a hop
    #
    def _job_get_exit_code (self, id) :
        """ get the job's exit code from the wrapper shell """

        rm, pid = self._adaptor.parse_id (id)

        ret, out, _ = self.shell.run_sync ("RESULT %s\n" % pid)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to get exit code for '%s': (%s)(%s)" \
                               % (id, ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if  len (lines) == 3 :
            # shell did not manage to do 'stty -echo'?
            del (lines[0])

        if  len (lines) != 2 :
            raise saga.NoSuccess ("failed to get exit code for '%s': (%s)" % (id, lines))

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to get exit code for '%s' (%s)" % (id, lines))

        exit_code = lines[1].strip ()

        if not exit_code.isdigit () :
            return None

        return int(exit_code)
        

    # ----------------------------------------------------------------
    #
    # TODO: this should also cache state
    #
    def _job_suspend (self, id) :

        rm, pid = self._adaptor.parse_id (id)

        ret, out, _ = self.shell.run_sync ("SUSPEND %s\n" % pid)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to suspend job '%s': (%s)(%s)" \
                               % (id, ret, out))


    # ----------------------------------------------------------------
    #
    # TODO: this should also cache state
    #
    def _job_resume (self, id) :

        rm, pid = self._adaptor.parse_id (id)

        ret, out, _ = self.shell.run_sync ("RESUME %s\n" % pid)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to resume job '%s': (%s)(%s)" \
                               % (id, ret, out))


    # ----------------------------------------------------------------
    #
    # TODO: this should also fetch the (final) state, to safe a hop
    #
    def _job_cancel (self, id) :

        rm, pid = self._adaptor.parse_id (id)

        ret, out, _ = self.shell.run_sync ("CANCEL %s\n" % pid)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to cancel job '%s': (%s)(%s)" \
                               % (id, ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if  len (lines) == 3 :
            # shell did not manage to do 'stty -echo'?
            del (lines[0])

        if  len (lines) != 2 :
            raise saga.NoSuccess ("failed to cancel job '%s': (%s)" % (id, lines))

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to cancel job '%s' (%s)" % (id, lines))



    # ----------------------------------------------------------------
    #
    # TODO: this should also fetch the (final) state, to safe a hop
    # TODO: implement via notifications
    #
    def _job_wait (self, id, timeout) :
        """ 
        A call to the shell to do the WAIT would block the shell for any
        other interactions.  In particular, it would practically kill it if the
        Wait waits forever...

        So we implement the wait via a state pull.  The *real* solution is, of
        course, to implement state notifications, and wait for such
        a notification to arrive within timeout seconds...
        """

        time_start = time.time ()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id (id)

        while True :
            state = self._job_get_state (id)
            if  state == saga.job.DONE      or \
                state == saga.job.FAILED    or \
                state == saga.job.CANCELED     :
                    return True
            # avoid busy poll
            time.sleep (0.5)

            # check if we hit timeout
            if  timeout >= 0 :
                time_now = time.time ()
                if  time_now - time_start > timeout :
                    return False

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run_job (self, cmd, host) :
        """ Implements saga.adaptors.cpi.job.Service.run_job()
        """

        if not cmd :
            raise saga.BadParameter._log (self._logger, "run_hosts needs a command to run")

        if "'" in cmd :
            raise saga.BadParameter._log (self._logger, "command cannot contain \"'\" (%s)"  \
                                       % cmd)

        if  host and host != self.rm.host :
            raise saga.BadParameter._log (self._logger, "Can only run jobs on %s"
                                       % self.rm.host)

        cmd_elems = cmd.split ()

        if not len(cmd_elems) :
            raise saga.BadParameter._log (self._logger, "run_hosts needs a non-empty command to run")

        jd = saga.job.Description ()

        jd.executable = "/bin/sh"
        jd.arguments  = ["-c", "'%s'" % " ".join (cmd_elems)]

        job = self.create_job (jd)
        job.run ()

        return job

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


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self):

        # FIXME: this should also fetch job state and metadata, and cache those

        ret, out, _ = self.shell.run_sync ("LIST\n")
        if  ret != 0 :
            raise saga.NoSuccess ("failed to list jobs: (%s)(%s)" \
                               % (ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to list jobs (%s)" % (lines))

        del lines[0]
        self._ids = []

        for line in lines :
            try :
                pid    = int(line.strip ())
                job_id = "[%s]-[%s]" % (self.rm, pid)
                self._ids.append (job_id)
            except Exception as e:
                self._logger.error ("Ignore non-int job pid (%s) (%s)" % (line, e))

        return self._ids
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job (self, jobid):
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """

        known_jobs = self.list ()

        if jobid not in known_jobs :
            raise saga.BadParameter._log (self._logger, "job id %s unknown"
                                       % jobid)

        else:
            # this dict is passed on to the job adaptor class -- use it to pass any
            # state information you need there.
            adaptor_state = { "job_service"     : self, 
                              "job_id"          : jobid,
                              "job_schema"      : self.rm.schema }

            return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=adaptor_state)

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_run (self, jobs) :
        """
        From all the job descriptions in the container, build a bulk, and submit
        as async.  The read whaterver the wrapper returns, and sort through the
        messages, assigning job IDs etc.
        """

        self._logger.debug ("container run: %s"  %  str(jobs))

        bulk = "BULK\n"

        for job in jobs :
            cmd   = self._jd2cmd (job.description)
            bulk += "RUN %s\n" % cmd

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to run job: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to run job : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to run job : (%s)(%s)" % (ret, out))
                continue

            # FIXME: verify format of returned pid (\d+)!
            pid    = lines[-1].strip ()
            job_id = "[%s]-[%s]" % (self.rm, pid)

            self._logger.debug ("started job %s" % job_id)

            self.njobs += 1

            # FIXME: at this point we need to make sure that we actually created
            # the job.  Well, we should make sure of this *before* we run it.
            # But, actually, the container sorter should have done that already?
            # Check!
            job._adaptor._id = job_id

        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to run (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job submission: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to run (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_wait (self, jobs, mode, timeout) :

        self._logger.debug ("container wait: %s"  %  str(jobs))

        bulk = "BULK\n"

        for job in jobs :
            rm, pid = self._adaptor.parse_id (job.id)
            bulk   += "WAIT %s\n" % pid

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to wait for job: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to wait for job : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to wait for job : (%s)(%s)" % (ret, out))
                continue

        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to wait for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job wait: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to wait for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_cancel (self, jobs, timeout) :

        self._logger.debug ("container cancel: %s"  %  str(jobs))

        bulk = "BULK\n"

        for job in jobs :
            rm, pid = self._adaptor.parse_id (job.id)
            bulk   += "CANCEL %s\n" % pid

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to cancel job: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to cancel job : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to cancel job : (%s)(%s)" % (ret, out))
                continue

        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to cancel (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job cancel: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to cancel (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_get_states (self, jobs) :

        self._logger.debug ("container get_state: %s"  %  str(jobs))

        bulk   = "BULK\n"
        states = []

        for job in jobs :
            rm, pid = self._adaptor.parse_id (job.id)
            bulk   += "STATE %s\n" % pid

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to get job state: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to get job state : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to get job state : (%s)(%s)" % (ret, out))
                continue

            state = self._adaptor.string_to_state (lines[-1])

            job._adaptor._state = state
            states.append (state)


        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to get state for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job get_state: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to get state for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        return states


###############################################################################
#
class ShellJob (saga.adaptors.cpi.job.Job) :
    """ Implements saga.adaptors.cpi.job.Job
    """
    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (ShellJob, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, job_info):
        """ Implements saga.adaptors.cpi.job.Job.init_instance()
        """

        if  'job_description' in job_info :
            # comes from job.service.create_job()
            self.js = job_info["job_service"] 
            self.jd = job_info["job_description"]

            # the js is responsible for job bulk operations -- which
            # for jobs only work for run()
            self._container       = self.js
            self._method_type     = "run"

            # initialize job attribute values
            self._id              = None
            self._state           = saga.job.NEW
            self._exit_code       = None
            self._exception       = None

        elif 'job_id' in job_info :
            # initialize job attribute values
            self.js               = job_info["job_service"] 
            self._id              = job_info['job_id']
            self._state           = saga.job.UNKNOWN
            self._exit_code       = None
            self._exception       = None

        else :
            # don't know what to do...
            raise saga.BadParameter ("Cannot create job, insufficient information")
        
        return self.get_api ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description (self):
        return self.jd


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state (self):
        """ Implements saga.adaptors.cpi.job.Job.get_state()
        """

        # we may not yet have a backend representation...
        try :
            self._state = self.js._job_get_state (self._id)
            return self._state
        except Exception as e :
            if self._id == None :
                # no ID yet, we should still have New state...
                return self._state
            else :
                raise e


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url (self):

        if not self.js :
            raise saga.IncorrectState ("Job Service URL unknown")
        else :
            return self.js.get_url ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait (self, timeout):
        return self.js._job_wait (self._id, timeout)

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_id() """        
        return self._id
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_exit_code() """

        if self._exit_code != None :
            return self._exit_code

        self._exit_code = self.js._job_get_exit_code (self._id)

        return self._exit_code
   
    # ----------------------------------------------------------------
    #
    # TODO: the values below should be fetched with every get_state...
    #
    @SYNC_CALL
    def get_execution_hosts (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """        
        self._logger.debug ("this is the shell adaptor, reporting execution hosts")
        return [self.js.get_url ().host]
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run (self): 
        self._id = self.js._job_run (self.jd)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def suspend (self):
        self.js._job_suspend (self._id)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def resume (self):
        self.js._job_resume (self._id)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel (self, timeout):
        self.js._job_cancel (self._id)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def re_raise (self):
        # nothing to do here actually, as run () is synchronous...
        return self._exception


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

