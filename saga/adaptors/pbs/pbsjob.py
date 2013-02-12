
#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

""" PBS job adaptor implementation
"""

import saga.utils.which
import saga.utils.pty_shell

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job

from saga.job.constants import *

import re
import time
from copy import deepcopy

import ssh_wrapper

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.pbsjob"
_ADAPTOR_SCHEMAS       = ["pbs", "pbs+ssh", "pbs+gsissh"]
_ADAPTOR_OPTIONS       = [
    {
    'category':      'saga.adaptor.pbsjob',
    'name':          'foo',
    'type':          bool,
    'default':       False,
    'valid_options': [True, False],
    'documentation': """Create a detailed debug trace on the remote host.
                        Note that the log is *not* removed, and can be large!
                        A log message on INFO level will be issued which
                        provides the location of the log file.""",
    'env_variable':   None
    },
]

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "jdes_attributes":   [saga.job.EXECUTABLE,
                          saga.job.ARGUMENTS,
                          saga.job.ENVIRONMENT,
                          saga.job.INPUT,
                          saga.job.OUTPUT,
                          saga.job.ERROR,
                          saga.job.QUEUE,
                          saga.job.PROJECT],
    "job_attributes":    [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    "metrics":           [saga.job.STATE,
                          saga.job.STATE_DETAIL],
    "contexts":          {"ssh":      "SSH public/private keypair",
                          "x509":     "X509 proxy for gsissh",
                          "userpass": "username/password pair for simple ssh"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "cfg_options":   _ADAPTOR_OPTIONS,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":   """The PBS job adaptor. This adaptor can run jobs on
                        PBS clusters.""",
    "details": """ A more elaborate description....""",
    "schemas": {"pbs":   "use a local PBS installations",
               "pbs+ssh":    "use ssh to conenct to a remote PBS cluster",
               "pbs+gsissh": "use gsissh to connect to a remote PBS cluster"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name":    _ADAPTOR_NAME,
    "version": "v0.1",
    "schemas": _ADAPTOR_SCHEMAS,
    "cpis": [
        {
        "type": "saga.job.Service",
        "class": "PBSJobService"
        },
        {
        "type": "saga.job.Job",
        "class": "PBSJob"
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
    def __init__(self):

        saga.adaptors.cpi.base.AdaptorBase.__init__(self,
            _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.opts  = self.get_config()

        self.foo   = self.opts['foo'].get_value()

        #self._logger.info('debug trace : %s' % self.debug_trace)

    # ----------------------------------------------------------------
    #
    def sanity_check(self):

        # FIXME: also check for gsissh

        pass

    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if not match or len(match.groups()) != 2:
            raise saga.BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


###############################################################################
#
class PBSJobService (saga.adaptors.cpi.job.Service):
    """ Implements saga.adaptors.cpi.job.Service """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._cpi_base = super(PBSJobService, self)
        self._cpi_base.__init__(api, adaptor)

    # ----------------------------------------------------------------
    #
    def __del__(self):

        # FIXME: not sure if we should PURGE here -- that removes states which
        # might not be evaluated, yet.  Should we mark state evaluation
        # separately?
        #   cmd_state () { touch $DIR/purgeable; ... }
        # When should that be done?
        self._logger.error("adaptor dying... %s" % self.njobs)
        self._logger.trace()

        #     try :
        #       # if self.shell : self.shell.run_sync ("PURGE", iomode=None)
        #         if self.shell : self.shell.run_sync ("QUIT" , iomode=None)
        #     except :
        #         pass

        self.finalize(kill_shell=True)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ Service instance constructor """

        self.rm      = rm_url
        self.session = session
        self.njobs   = 0

        rm_scheme = rm_url.scheme
        pty_url   = deepcopy(rm_url)

        # we need to extrac the scheme for PTYShell. That's basically the
        # job.Serivce Url withou the pbs+ part. We use the PTYShell to execute
        # pbs commands either locally or via gsissh or ssh.
        if rm_scheme == "pbs":
            pty_url.scheme = "fork"
        elif rm_scheme == "pbs+ssh":
            pty_url.scheme = "ssh"
        elif rm_scheme == "pbs+gsissh":
            pty_url.scheme = "gsissh"

        self.shell = saga.utils.pty_shell.PTYShell(pty_url,
            self.session.contexts, self._logger)

        self.shell.set_initialize_hook(self.initialize)
        self.shell.set_finalize_hook(self.finalize)

        self.initialize()

    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # start the shell, find its prompt.  If that is up and running, we can
        # bootstrap our wrapper script, and then run jobs etc.

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

        # if debug_trace is set, we add some magic redirection to the shell
        # command: we run it with 'sh -x' which creates a trace, and pipe to
        # 'tree $base/wrapper.$$.log', which will save the trace in the log
        # file.  We still need the wrapper's output on stdout, so we filter out
        # the trace from the output (grep -v -e '^\\+'), and send the remainder
        # to the default output channel
        shell = '/bin/sh'
        redir = ''
        #if self._adaptor.debug_trace :
        #    trace  = "%s/wrapper.$$.log" % base
        #    shell += " -x"
        #    redir  = "2>&1 | tee %s | grep -v -e '^\\+'" % trace


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
        ret, out, _ = self.shell.run_sync ("/bin/sh -c '(%s %s/wrapper.sh $$ && kill -9 $PPID) %s' || false" \
                                        % (shell, base, redir))

        # either way, we somehow ran the script, and just need to check if it
        # came up all right...
        if  ret != 0 :
            raise saga.NoSuccess ("failed to run wrapper (%s)(%s)" % (ret, out))

        # if debug trace was requested, we now should know its name and can
        # report it.
        #if self._adaptor.debug_trace :
#
 #           wrapper_pid = '?'
  #          for line in out.split ('\n') :
#
 #               if re.match ('^PID: \d+$', line) :
  #                  wrapper_pid = line[5:]
   #           
    #        trace = "%s/wrapper.%s.log" % (base, wrapper_pid)
     #       self._logger.error ('remote trace: %s : %s', self.rm, trace)

        self._logger.debug ("got cmd prompt (%s)(%s)" % (ret, out))


    # ----------------------------------------------------------------
    #
    def finalize (self, kill_shell = False) :

        # print "ssh shell finalize"

        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)
                # print "ssh shell finalize 1"

        # print "ssh shell finalize done"


    # ----------------------------------------------------------------
    #
    #
    def _job_run (self, jd) :
        """ runs a job on the wrapper via pty, and returns the job id """

        exe = jd.executable
        arg = ""
        env = ""
        cwd = ""

        if jd.attribute_exists ("arguments") :
            for a in jd.arguments :
                arg += " %s" % a

        if jd.attribute_exists ("environment") :
            for e in jd.environment :
                env += "export %s=%s; "  %  (e, jd.environment[e])

        if jd.attribute_exists ("working_directory") :
            cwd = "cd %s && " % jd.working_directory

        cmd = "(%s %s %s %s)"  %  (env, cwd, exe, arg)

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

        rm, pid = self._adaptor.parse_id (id)

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

        return lines[1].strip ()
        

    # ----------------------------------------------------------------
    #
    # TODO: this should also fetch the (final) state, to safe a hop
    #
    def _job_get_exit_code (self, id) :
        """ get the job's exit code from the wrapper shell """

        rm, pid = self._adaptor.parse_id (id)

        ret, out, _ = self.shell.run_sync ("RESULT %s\n" % pid)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to get job exit code for '%s': (%s)(%s)" \
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

        return lines[1].strip ()
        

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

        if  len (lines) == 2 :
            # shell did not manage to do 'stty -echo'?
            del (lines[0])

        if  len (lines) != 1 :
            raise saga.NoSuccess ("failed to get job state for '%s': (%s)" % (id, lines))

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to get valid job state for '%s' (%s)" % (id, lines))


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
            if  state == 'DONE'     or \
                state == 'FAILED'   or \
                state == 'CANCELED'    :
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
   
   
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_job (self, jobid):
  #     """ Implements saga.adaptors.cpi.job.Service.get_url()
  #     """
  #     if jobid not in self._jobs.values ():
  #         msg = "Service instance doesn't know a Job with ID '%s'" % (jobid)
  #         raise saga.BadParameter._log (self._logger, msg)
  #     else:
  #         for (job_obj, job_id) in self._jobs.iteritems ():
  #             if job_id == jobid:
  #                 return job_obj.get_api ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # def container_run (self, jobs) :
  #     self._logger.debug ("container run: %s"  %  str(jobs))
  #     # TODO: this is not optimized yet
  #     for job in jobs:
  #         job.run ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # def container_wait (self, jobs, mode, timeout) :
  #     self._logger.debug ("container wait: %s"  %  str(jobs))
  #     # TODO: this is not optimized yet
  #     for job in jobs:
  #         job.wait ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # def container_cancel (self, jobs) :
  #     self._logger.debug ("container cancel: %s"  %  str(jobs))
  #     raise saga.NoSuccess ("Not Implemented");


###############################################################################
#
class PBSJob (saga.adaptors.cpi.job.Job):
    """ Implements saga.adaptors.cpi.job.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        self._cpi_base = super(PBSJob, self)
        self._cpi_base.__init__(api, adaptor)

    @SYNC_CALL
    def init_instance(self, job_info):
        """ Implements saga.adaptors.cpi.job.Job.init_instance()
        """
        # init_instance is called for every new saga.job.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        # the js is responsible for job bulk operations -- which
        # for jobs only work for run()
      # self._container       = self.js
        self._method_type     = "run"

        # initialize job attribute values
        self._id              = None
        self._state           = saga.job.NEW
        self._exit_code       = None
        self._exception       = None
        self._started         = None
        self._finished        = None
        
        return self.get_api ()


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
                return self._state
            else :
                raise e

  

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
   
  # # ----------------------------------------------------------------
  # #
  # # TODO: the values below should be fetched with every get_state...
  # #
  # @SYNC_CALL
  # def get_created (self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_started()
  #     """     
  #     # for local jobs started == created. for other adaptors 
  #     # this is not necessarily true   
  #     return self._started
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_started (self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_started()
  #     """        
  #     return self._started
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_finished (self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_finished()
  #     """        
  #     return self._finished
  # 
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_execution_hosts (self) :
  #     """ Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
  #     """        
  #     return self._execution_hosts
  #
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel (self, timeout):
        self._id = self.js._job_cancel (self.jd)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run (self): 
        self._id = self.js._job_run (self.jd)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def re_raise (self):
        # nothing to do here actually, as run () is synchronous...
        return self._exception


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

