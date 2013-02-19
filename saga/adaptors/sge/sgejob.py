#!/usr/bin/env python
# encoding: utf-8

""" SGE job adaptor implementation
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

SYNC_CALL = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    logger.error(message)
    raise exception(message)


# --------------------------------------------------------------------
#
def _sge_to_saga_jobstate(sgejs):
    """ translates a sge one-letter state to saga
    """
    if sgejs == 'c':
        return saga.job.DONE
    elif sgejs == 'E':
        return saga.job.RUNNING
    elif sgejs == 'H':
        return saga.job.PENDING
    elif sgejs == 'qw':
        return saga.job.PENDING
    elif sgejs == 'r':
        return saga.job.RUNNING
    elif sgejs == 't':
        return saga.job.RUNNING
    elif sgejs == 'w':
        return saga.job.PENDING
    elif sgejs == 's':
        return saga.job.PENDING
    elif sgejs == 'X':
        return saga.job.CANCELED
    elif sgejs == 'Eqw':
        return saga.job.FAILED
    else:
        return saga.job.UNKNOWN


# --------------------------------------------------------------------
#
def _sgescript_generator(url, logger, jd, ppn):
    """ generates an SGE script from a SAGA job description
    """
    sge_params = str()
    exec_n_args = str()

    if jd.executable is not None:
        exec_n_args += "%s " % (jd.executable)
    if jd.arguments is not None:
        for arg in jd.arguments:
            exec_n_args += "%s " % (arg)

    if jd.name is not None:
        sge_params += "#$ -N %s \n" % jd.name

    sge_params += "#$ -V \n"

    if jd.environment is not None:
        variable_list = str()
        for key in jd.environment.keys():
            variable_list += "%s=%s," % (key, jd.environment[key])
        sge_params += "#$ -v %s \n" % variable_list

    if jd.working_directory is not None:
        sge_params += "#$ -wd %s \n" % jd.working_directory
    if jd.output is not None:
        sge_params += "#$ -o %s \n" % jd.output
    if jd.error is not None:
        sge_params += "#$ -e %s \n" % jd.error
    if jd.wall_time_limit is not None:
        hours = jd.wall_time_limit / 60
        minutes = jd.wall_time_limit % 60
        sge_params += "#$ -l h_rt=%s:%s:00 \n" % (str(hours), str(minutes))
    if jd.queue is not None:
        sge_params += "#$ -q %s \n" % jd.queue
    else:
        raise Exception("No queue defined.")

    if jd.project is not None:
        sge_params += "#$ -A %s \n" % str(jd.project)
    if jd.job_contact is not None:
        sge_params += "#$ -m be \n"
        sge_params += "#$ -M %s \n" % jd.contact

    # if no cores are requested at all, we default to one
    if jd.total_cpu_count is None:
        jd.total_cpu_count = 1

    # we need to translate the # cores requested into
    # multiplicity, i.e., if one core is requested and
    # the cluster consists of 16-way SMP nodes, we will
    # request 16. If 17 cores are requested, we will
    # request 32... and so on ... self.__ppn represents
    # the core count per single node
    count = int(int(jd.total_cpu_count) / int(ppn))
    if int(jd.total_cpu_count) % int(ppn) != 0:
        count = count + 1
    count = count * int(ppn)

    sge_params += "#$ -pe %sway %s" % (ppn, str(count))

    sgescript = "\n#!/bin/bash \n%s \n%s" % (sge_params, exec_n_args)
    return sgescript

# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.sgejob"
_ADAPTOR_SCHEMAS       = ["sge", "sge+ssh", "sge+gsissh"]
_ADAPTOR_OPTIONS       = [
    {
    'category':      'saga.adaptor.sgejob',
    'name':          'foo',
    'type':          bool,
    'default':       False,
    'valid_options': [True, False],
    'documentation': """Doc""",
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
                          saga.job.PROJECT,
                          saga.job.WALL_TIME_LIMIT,
                          saga.job.WORKING_DIRECTORY,
                          saga.job.TOTAL_CPU_COUNT],
    "job_attributes":    [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    "metrics":           [saga.job.STATE],
    "contexts":          {"ssh": "SSH public/private keypair",
                          "x509": "GSISSH X509 proxy context",
                          "userpass": "username/password pair (ssh)"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "cfg_options":   _ADAPTOR_OPTIONS,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":   """The SGE adaptor can run and manage jobs on local and
                        remote SGE clusters.""",
    "details": """TODO""",
    "schemas": {"sge":        "connect to a local SGE cluster",
                "sge+ssh":    "conenct to a remote SGE cluster via SSH",
                "sge+gsissh": "connect to a remote SGE cluster via GSISSH"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {
    "name":    _ADAPTOR_NAME,
    "version": "v0.1",
    "schemas": _ADAPTOR_SCHEMAS,
    "cpis": [
        {
        "type": "saga.job.Service",
        "class": "SGEJobService"
        },
        {
        "type": "saga.job.Job",
        "class": "SGEJob"
        }
    ]
}


###############################################################################
# The adaptor class
class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by 
        the SAGA engine), and which registers the CPI implementation classes 
        which provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self):

        saga.adaptors.cpi.base.AdaptorBase.__init__(self,
            _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.opts = self.get_config()
        self.foo = self.opts['foo'].get_value()

        #self._logger.info('debug trace : %s' % self.debug_trace)

    # ----------------------------------------------------------------
    #
    def sanity_check(self):
        # FIXME: also check for gsissh
        pass

    # ----------------------------------------------------------------
    #
    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if not match or len(match.groups()) != 2:
            raise saga.BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


###############################################################################
#
class SGEJobService (saga.adaptors.cpi.job.Service):
    """ implements saga.adaptors.cpi.job.Service 
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._cpi_base = super(SGEJobService, self)
        self._cpi_base.__init__(api, adaptor)

    # ----------------------------------------------------------------
    #
    def __del__(self):

        # FIXME: not sure if we should PURGE here -- that removes states which
        # might not be evaluated, yet.  Should we mark state evaluation
        # separately?
        #   cmd_state () { touch $DIR/purgeable; ... }
        # When should that be done?
        # self._logger.error("adaptor dying... %s" % self.njobs)
        # self._logger.trace()

        self.finalize(kill_shell=True)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm      = rm_url
        self.session = session
        self.ppn     = 0
        self.jobs    = dict()

        rm_scheme = rm_url.scheme
        pty_url   = deepcopy(rm_url)

        # we need to extrac the scheme for PTYShell. That's basically the
        # job.Serivce Url withou the sge+ part. We use the PTYShell to execute
        # pbs commands either locally or via gsissh or ssh.
        if rm_scheme == "sge":
            pty_url.scheme = "fork"
        elif rm_scheme == "sge+ssh":
            pty_url.scheme = "ssh"
        elif rm_scheme == "sge+gsissh":
            pty_url.scheme = "gsissh"

        # these are the commands that we need in order to interact with SGE.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note avaialbe.
        self._commands = {'qstat': None,
                          'qsub':  None,
                          'qdel':  None,
                          'qconf': None}

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
        # check if all required sge tools are available
        for cmd in self._commands.keys():
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding SGE tools: %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
            else:
                path = out.strip()  # strip removes newline

                ret, out, _ = self.shell.run_sync("%s -help" % cmd)
                if ret != 0:
                    message = "Error finding SGE tools: %s" % out
                    log_error_and_raise(message, saga.NoSuccess,
                        self._logger)
                else:
                    # version is reported in the first row of the
                    # help screen, e.g., GE 6.2u5_1
                    version = out.strip().split('\n')[0]

                    # add path and version to the command dictionary
                    self._commands[cmd] = {"path":    path,
                                           "version": version}

        self._logger.info("Found SGE tools: %s" % self._commands)

        # see if we can get some information about the cluster, e.g.,
        # different queues, number of processes per node, etc.
        # TODO: this is quite a hack. however, it *seems* to work quite
        #       well in practice.
        ret, out, _ = self.shell.run_sync('%s -sq %s | grep slots' % \
            (self._commands['qconf']['path'], 'normal'))
        if ret != 0:
            message = "Error running 'qconf': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # this is black magic. we just assume that the highest occurence
            # of a specific np is the number of processors (cores) per compute
            # node. this equals max "PPN" for job scripts
            self.ppn = out.split()[1]
            self._logger.debug("Determined 'wayness' for queue '%s': %s" \
                % ('normal', self.ppn))

    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        pass

    # ----------------------------------------------------------------
    #
    def _job_run(self, jd):
        """ runs a job via qsub
        """

        # create an SGE job script from SAGA job description
        script = _sgescript_generator(url=self.rm, logger=self._logger,
            jd=jd, ppn=self.ppn)
        self._logger.debug("Generated SGE script: %s" % script)

        ret, out, _ = self.shell.run_sync("echo '%s' | %s" \
            % (script, self._commands['qsub']['path']))

        if ret != 0:
            # something went wrong
            message = "Error running job via 'qsub': %s. Script was: %s" \
                % (out, script)
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # stdout contains the job id:
            # Your job 1036608 ("testjob") has been submitted
            pid = None
            for line in out.split('\n'):
                if line.find("Your job") != -1:
                    pid = line.split()[2]
            if pid == None:
                message = "Couldn't parse job id from 'qsub' output: %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)

            job_id = "[%s]-[%s]" % (self.rm, pid)
            self._logger.info("Submitted SGE job with id: %s" % job_id)

            # add job to internal list of jobs.
            self.jobs[job_id] = saga.job.PENDING

            return job_id

    # ----------------------------------------------------------------
    #
    def _job_get_state(self, jobid):
        """ get job state via qstat
        """
        job_state = None

        rm, pid = self._adaptor.parse_id(jobid)
        # get the job status via qstat
        ret, out, _ = self.shell.run_sync("%s | grep %s" \
            % (self._commands['qstat']['path'], pid))
        if ret != 0:
            # if the job existed previously, it has disappeared from the
            # queueing system. in that case we can set the job_state to
            # DONE. the job could also have failed, but that's impossible
            # to figure out.
            if jobid in self.jobs:
                if self.jobs[jobid] == saga.job.RUNNING:
                    self.jobs[jobid] = saga.job.DONE
                    job_state = self.jobs[jobid]
                else:
                    job_state = self.jobs[jobid]
            else:
                # something else went wrong
                message = "Error retrieving job status via 'qstat': %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # result should look like this:
            # 1036615 0.51206 testjob  tg802352  qw  02/19/2013 02:41:08
            job_state = _sge_to_saga_jobstate(out.split()[4])

        return job_state

    # ----------------------------------------------------------------
    #
    def _job_get_info(self, jobid):
        """ get job attributes via qstat
        """
        job_state   = None
        exec_hosts  = None
        exit_status = None
        create_time = None
        start_time  = None
        end_time    = None

        rm, pid = self._adaptor.parse_id(jobid)

        # get the job status via qstat
        ret, out, _ = self.shell.run_sync("%s | grep %s" \
            % (self._commands['qstat']['path'], pid))
        if ret != 0:
            # if the job existed previously, it has disappeared from the
            # queueing system. in that case we can set the job_state to
            # DONE. the job could also have failed, but that's impossible
            # to figure out.
            if jobid in self.jobs:
                if self.jobs[jobid] == saga.job.RUNNING:
                    self.jobs[jobid] = saga.job.DONE
                    job_state = self.jobs[jobid]
                else:
                    job_state = self.jobs[jobid]
            else:
                # something else went wrong
                message = "Error retrieving job status via 'qstat': %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # result should look like this:
            # 1036615 0.51206 testjob  tg802352  qw  02/19/2013 02:41:08
            job_state = _sge_to_saga_jobstate(out.split()[4])

        # now we run 
        ret, out, _ = self.shell.run_sync("%s -f1 %s | \
            egrep '(job_state)|(exec_host)|(exit_status)|(ctime)|(start_time)|(comp_time)'" \
            % (self._commands['qstat']['path'], pid))

        #if ret != 0:
        #    # something went wrong
        #    message = "Error retrieving job info via 'qstat': %s" % out
        #    log_error_and_raise(message, saga.NoSuccess, self._logger)

        # parse the egrep result. this should look something like this:
        #     job_state = C
        #     exec_host = i72/0
        #     exit_status = 0
        #results = out.split('\n')
        #for result in results:
        #    if len(result.split('=')) == 2:
        #        key, val = result.split('=')
        #        key = key.strip()  # strip() removes whitespaces at the
        #        val = val.strip()  # beginning and the end of the string

        #        if key == 'job_state':
        #            job_state = _sge_to_saga_jobstate(val)
        #        elif key == 'exec_host':
        #            exec_hosts = val.split('+')  # format i73/7+i73/6+...
        #        elif key == 'exit_status':
        #            exit_status = val
        #        elif key == 'ctime':
        #            create_time = val
        #        elif key == 'start_time':
        #            start_time = val
        #        elif key == 'comp_time':
        #            end_time = val

        return (job_state, exec_hosts, exit_status,
                create_time, start_time, end_time)

    # ----------------------------------------------------------------
    #
    def _job_get_exit_code(self, id):
        """ get the job's exit code
        """
        # _job_get_info returns (job_state, exec_hosts, exit_status,
        #                        create_time, start_time)
        return self._job_get_info(id)[2]

    # ----------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, id):
        """ get the job's exit code
        """
        # _job_get_info returns (job_state, exec_hosts, exit_status,
        #                        create_time, start_time)
        return self._job_get_info(id)[1]

    # ----------------------------------------------------------------
    #
    def _job_cancel(self, id):
        """ cancel the job via 'qdel'
        """
        rm, pid = self._adaptor.parse_id(id)

        ret, out, _ = self.shell.run_sync("%s %s\n" \
            % (self._commands['qdel']['path'], pid))

        if ret != 0:
            message = "Error canceling job via 'qdel': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)

    # ----------------------------------------------------------------
    #
    def _job_wait(self, id, timeout):
        """ wait for the job to finish or fail
        """

        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(id)

        while True:
            state = self._job_get_info(id)[0]
            if state == saga.job.DONE or \
               state == saga.job.FAILED or \
               state == saga.job.CANCELED:
                    return True
            # avoid busy poll
            time.sleep(0.5)

            # check if we hit timeout
            if timeout >= 0:
                time_now = time.time()
                if time_now - time_start > timeout:
                    return False

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        """ implements saga.adaptors.cpi.job.Service.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES["jdes_attributes"]:
                message = "'jd.%s' is not supported by this adaptor" \
                    % attribute
                log_error_and_raise(message, saga.BadParameter, self._logger)

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema
                        }

        return saga.job.Job(_adaptor=self._adaptor,
                            _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        """ implements saga.adaptors.cpi.job.Service.get_url()
        """
        return self.rm

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """ implements saga.adaptors.cpi.job.Service.list()
        """
        ids = []

        ret, out, _ = self.shell.run_sync("%s | grep `whoami`"\
            % self._commands['qstat']['path'])
        if ret != 0 and len(out) > 0:
            message = "failed to list jobs via 'qstat': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        elif ret != 0 and len(out) == 0:
            # qstat | grep `whoami` exits with 1 if the list is empty
            pass
        else:
            jobid = "[%s]-[%s]" % (self.rm, out.split()[0])
            ids.append(jobid)

        return ids

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
class SGEJob (saga.adaptors.cpi.job.Job):
    """ implements saga.adaptors.cpi.job.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        self._cpi_base = super(SGEJob, self)
        self._cpi_base.__init__(api, adaptor)

    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements saga.adaptors.cpi.job.Job.init_instance()
        """
        # init_instance is called for every new saga.job.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        # initialize job attribute values
        self._id              = None
        self._state           = saga.job.NEW
        self._exit_code       = None
        self._exception       = None
        self._created         = None
        self._started         = None
        self._finished        = None
        self._execution_hosts = None

        return self.get_api()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ mplements saga.adaptors.cpi.job.Job.get_state()
        """
        # if the state is DONE, CANCELED or FAILED, it is considered
        # final and we don't need to query the backend again
        if self._state == saga.job.CANCELED or self._state == saga.job.FAILED \
            or self._state == saga.job.DONE:
            return self._state
        else:
            try:
                self._state = self.js._job_get_state(self._id)
                return self._state
            except Exception as e:
                # the job can disappear on the remote end.
                if self._id == None:
                    return self._state
                else:
                    raise e

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements saga.adaptors.cpi.job.Job.wait()
        """
        return self.js._job_wait(self._id, timeout)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        """ implements saga.adaptors.cpi.job.Job.get_id()
        """
        return self._id

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        """ implements saga.adaptors.cpi.job.Job.get_exit_code()
        """
        if self._exit_code != None:
            return self._exit_code

        self._exit_code = self.js._job_get_exit_code(self._id)
        return self._exit_code

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """ implements saga.adaptors.cpi.job.Job.get_created()
        """
        if self._created is None:
            # _job_get_info returns (job_state, exec_hosts, exit_status,
            #                        create_time, start_time, end_time)
            self._created = self.js._job_get_info(self._id)[3]
        return self._created

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """ implements saga.adaptors.cpi.job.Job.get_started()
        """
        if self._started is None:
            # _job_get_info returns (job_state, exec_hosts, exit_status,
            #                        create_time, start_time, end_time)
            self._started = self.js._job_get_info(self._id)[4]
        return self._started

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """ implements saga.adaptors.cpi.job.Job.get_finished()
        """
        if self._finished is None:
            # _job_get_info returns (job_state, exec_hosts, exit_status,
            #                        create_time, start_time, end_time)
            self._finished = self.js._job_get_info(self._id)[5]
        return self._finished

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """ implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """
        if self._execution_hosts != None:
            return self._execution_hosts

        self._execution_hosts = self.js._job_get_execution_hosts(self._id)
        return self._execution_hosts

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements saga.adaptors.cpi.job.Job.cancel()
        """
        self.js._job_cancel(self._id)
        self._state = saga.job.CANCELED

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements saga.adaptors.cpi.job.Job.run()
        """
        self._id = self.js._job_run(self.jd)
        # after a call to run(), the job is either in RUNNIGN or
        # PENDING state.
        self._state = saga.job.PENDING

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def re_raise(self):
        # nothing to do here actually, as run () is synchronous...
        return self._exception
