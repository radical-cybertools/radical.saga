#!/usr/bin/env python
# encoding: utf-8

""" PBS job adaptor implementation
"""

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import saga.utils.which
import saga.utils.pty_shell

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job

from saga.job.constants import *

import re
import time
from copy import deepcopy

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


def log_error_and_raise(message, exception, logger):
    logger.error(message)
    raise exception(message)


def _pbscript_generator(url, logger, jd):
    '''Generates a PBS script from a SAGA job description.
    '''
    pbs_params = str()
    exec_n_args = str()

    if jd.executable is not None:
        exec_n_args += "%s " % (jd.executable)
    if jd.arguments is not None:
        for arg in jd.arguments:
            exec_n_args += "%s " % (arg)

    if jd.name is not None:
        pbs_params += "#PBS -N %s \n" % jd.name

    pbs_params += "#PBS -V     \n"

    if jd.environment is not None:
        variable_list = str()
        for key in jd.environment.keys():
            variable_list += "%s=%s," % (key, jd.environment[key])
        pbs_params += "#PBS -v %s \n" % variable_list

    if jd.working_directory is not None:
        pbs_params += "#PBS -d %s \n" % jd.working_directory
    if jd.output is not None:
        pbs_params += "#PBS -o %s \n" % jd.output
    if jd.error is not None:
        pbs_params += "#PBS -e %s \n" % jd.error
    if jd.wall_time_limit is not None:
        hours = jd.wall_time_limit / 60
        minutes = jd.wall_time_limit % 60
        pbs_params += "#PBS -l walltime=%s:%s:00 \n" \
            % (str(hours), str(minutes))
    if jd.queue is not None:
        pbs_params += "#PBS -q %s \n" % jd.queue
    if jd.project is not None:
        pbs_params += "#PBS -A %s \n" % str(jd.project)
    if jd.job_contact is not None:
        pbs_params += "#PBS -m abe \n"

    if url.scheme in ["xt5torque", "xt5torque+ssh", 'xt5torque+gsissh']:
        # Special case for TORQUE on Cray XT5s
        logger.info("Using Cray XT5 spepcific modifications, i.e., -l size=xx instead of -l nodes=x:ppn=yy ")
        if jd.total_cpu_count is not None:
            pbs_params += "#PBS -l size=%s" % jd.total_cpu_count
    else:
        # Default case (non-XT5)
        if jd.total_cpu_count is not None:
            tcc = int(jd.total_cpu_count)
            tbd = float(tcc) / float(self._ppn)
            if float(tbd) > int(tbd):
                pbs_params += "#PBS -l nodes=%s:ppn=%s" \
                    % (str(int(tbd) + 1), self._ppn)
            else:
                pbs_params += "#PBS -l nodes=%s:ppn=%s" \
                    % (str(int(tbd)), self._ppn)

    pbscript = "\n#!/bin/bash \n%s%s" % (pbs_params, exec_n_args)
    return pbscript


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
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [saga.job.NAME,
                          saga.job.EXECUTABLE,
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

_ADAPTOR_INFO = {
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

        # these are the commands that we need in order to interact with PBS.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note avaialbe.
        self._commands = {'pbsnodes': None,
                          'qstat':    None,
                          'qsub':     None}

        # create a null logger to silence the PTY wrapper!
        import logging
        class NullHandler(logging.Handler):
            def emit(self, record):
                pass
        nh = NullHandler()
        null_logger = logging.getLogger("PTYShell").addHandler(nh)

        self.shell = saga.utils.pty_shell.PTYShell(pty_url,
            self.session.contexts, null_logger)

        self.shell.set_initialize_hook(self.initialize)
        self.shell.set_finalize_hook(self.finalize)

        self.initialize()

    # ----------------------------------------------------------------
    #
    def initialize(self):

        # check if all required pbs tools are available
        for cmd in self._commands.keys():
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding PBS tools: %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
            else:
                path = out.strip()  # strip removes newline
                ret, out, _ = self.shell.run_sync("%s --version" % cmd)
                if ret != 0:
                    message = "Error finding PBS tools: %s" % out
                    log_error_and_raise(message, saga.NoSuccess, self._logger)
                else:
                    # version is reported as: "version: x.y.z"
                    version = out.strip().split()[1]  # strip removes newline

                    # add path and version to the command dictionary
                    self._commands[cmd] = {"path":    path,
                                           "version": version}

        # TODO: detect any other specialties of the PBS installation ?

        self._logger.info("Found PBS tools: %s" % self._commands)

    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        pass

    # ----------------------------------------------------------------
    #
    def _job_run(self, jd):
        """ runs a job via PBS """

        # create a PBS job script from SAGA job description
        script = _pbscript_generator(url=self.rm, logger=self._logger, jd=jd)
        self._logger.debug("Generated PBS script: %s" % script)

        ret, out, _ = self.shell.run_sync("echo '%s' | %s" \
            % (script, self._commands['qsub']['path']))

        if ret != 0:
            # something went wrong
            message = "Error running 'qsub': %s. Script was: %s" \
                % (out, script)
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # stdout contains the job id
            job_id = "[%s]-[%s]" % (self.rm, out.strip())
            self._logger.info("Submitted PBS job with id: %s" % job_id)
            self.njobs += 1
            return job_id

    # ----------------------------------------------------------------
    #
    def _job_get_state(self, id):
        """ get the job state from the wrapper shell """

        rm, pid = self._adaptor.parse_id(id)

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
        """ get the job's exit code from the got initial shell promptapper shell """

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
