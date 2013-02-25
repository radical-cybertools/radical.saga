#!/usr/bin/env python
# encoding: utf-8

""" Condor job adaptor implementation
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
from cgi import parse_qs

SYNC_CALL = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    logger.error(message)
    raise exception(message)


# --------------------------------------------------------------------
#
def _condor_to_saga_jobstate(condorjs):
    """ translates a condor one-letter state to saga
    """
    # From: http://pages.cs.wisc.edu/~adesmet/status.html
    #
    # JobStatus in job ClassAds
    #
    # 0   Unexpanded  U
    # 1   Idle    I
    # 2   Running R
    # 3   Removed X
    # 4   Completed   C
    # 5   Held    H
    # 6   Submission_err  E

    if int(cdrjs) == 0:
        return saga.job.Pending
    elif int(cdrjs) == 1:
        return saga.job.Pending
    elif int(cdrjs) == 2:
        return saga.job.Running
    elif int(cdrjs) == 3:
        return saga.job.Canceled
    elif int(cdrjs) == 4:
        return saga.job.Done
    elif int(cdrjs) == 5:
        return saga.job.Pending
    elif int(cdrjs) == 6:
        return saga.job.Failed
    else:
        return saga.job.Unknown


# --------------------------------------------------------------------
#
def _condorscript_generator(url, logger, jd, query=None):
    """ generates a Condor script from a SAGA job description
    """
    condor_file = str()

    ##### OPTIONS PASSED VIA JOB SERVICE URL #####
    ##
    if query is not None:
        condor_file += "\n##### DEFAULT OPTIONS PASSED VIA JOB SERVICE URL #####\n##"
        # special treatment for universe - defaults to 'vanilla'
        if 'universe' not in query:
            condor_file += "\nuniverse = vanilla"
        for (key, value) in query.iteritems():
            condor_file += "\n%s = %s" % (key, value)

    ##### OPTIONS PASSED VIA JOB DESCRIPTION #####
    ##
    condor_file += "\n\n##### OPTIONS PASSED VIA JOB SERVICE URL #####\n##"
    requirements = "requirements = "

    # executable -> executable
    if jd.executable is not None:
        condor_file += "\nexecutable = %s" % jd.executable

    # arguments -> arguments
    arguments = "arguments = "
    if jd.arguments is not None:
        for arg in jd.arguments:
            arguments += "%s " % (arg)
    condor_file += "\n%s" % arguments

    # file_transfer -> transfer_input_files
    if jd.file_transfer is not None:
        td = TransferDirectives(jd.file_transfer)

        if len(td.in_append_dict) > 0:
            raise Exception('FileTransfer append syntax (>>) not supported by Condor: %s' % td.in_append_dict)
        if len(td.out_append_dict) > 0:
            raise Exception('FileTransfer append syntax (<<) not supported by Condor: %s' % td.out_append_dict)
        
        if len(td.in_overwrite_dict) > 0:
            transfer_input_files = "transfer_input_files = "
            for (source, target) in td.in_overwrite_dict.iteritems():
                # make sure source is file an not dir
                (s_path, s_entry) = os.path.split(source)
                if len(s_entry) < 1:
                    raise Exception('Condor accepts only files (not directories) as FileTransfer sources: %s' % source)
                # make sure target is just a file 
                (t_path, t_entry) = os.path.split(target)
                if len(t_path) > 1:
                    raise Exception('Condor accepts only filenames (without paths) as FileTransfer targets: %s' % target)
                # make sure source and target file are the same
                if s_entry != t_entry:
                    raise Exception('For Condor source file name and target file name have to be identical: %s != %s' % (s_entry, t_entry))
                # entry ok - add to job script
                transfer_input_files += "%s, " % source
            condor_file += "\n%s" % transfer_input_files

        if len(td.out_overwrite_dict) > 0:
            transfer_output_files = "transfer_output_files = "
            for (source, target) in td.out_overwrite_dict.iteritems():
                # make sure source is file an not dir
                (s_path, s_entry) = os.path.split(source)
                if len(s_entry) < 1:
                    raise Exception('Condor accepts only files (not directories) as FileTransfer sources: %s' % source)
                # make sure target is just a file 
                (t_path, t_entry) = os.path.split(target)
                if len(t_path) > 1:
                    raise Exception('Condor accepts only filenames (without paths) as FileTransfer targets: %s' % target)
                # make sure source and target file are the same
                if s_entry != t_entry:
                    raise Exception('For Condor source file name and target file name have to be identical: %s != %s' % (s_entry, t_entry))
                # entry ok - add to job script
                transfer_output_files += "%s, " % source
            condor_file += "\n%s" % transfer_output_files

    # always define log
    condor_file += "\nlog = saga-condor-job-$(cluster).log "

    # output -> output
    if jd.output is not None:
        condor_file += "\noutput = %s " % jd.output

    # error -> error
    if jd.error is not None:
        condor_file += "\nerror = %s " % jd.error 

    # environment -> environment
    environment = "environment = "
    if jd.environment is not None:
        variable_list = str()
        for key in jd.environment.keys(): 
            variable_list += "%s=%s;" % (key, jd.environment[key])
        environment += "%s " % variable_list
    condor_file += "\n%s" % environment

    # project -> +ProjectName
    if jd.project is not None:
        condor_file += "\n+ProjectName = \"%s\"" % str(jd.project)

    # candidate hosts -> SiteList + requirements
    if jd.candidate_hosts is not None:
        hosts = ""
        for host in jd.candidate_hosts:
            hosts += "%s, " % host
        sitelist = "+SiteList = \"%s\"" % hosts
        requirements += "(stringListMember(GLIDEIN_ResourceName,SiteList) == True)"
        condor_file += "\n%s" % sitelist
        condor_file += "\n%s" % requirements

    condor_file += "\n\nqueue"

    return condor_file


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.condorjob"
_ADAPTOR_SCHEMAS       = ["condor", "condor+ssh", "condor+gsissh"]
_ADAPTOR_OPTIONS       = [
    {
    'category':      'saga.adaptor.condorjob',
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
                          saga.job.CANDIDATE_HOSTS,
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
    "description":   """The Condor adaptor can run and manage jobs on local and
                        remote Condor gateways.""",
    "details": """TODO""",
    "schemas": {"condor":        "connect to a local Condor gateway",
                "condor+ssh":    "conenct to a remote Condor gateway via SSH",
                "condor+gsissh": "connect to a remote Condor gateway via GSISSH"}
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
        "class": "CondorJobService"
        },
        {
        "type": "saga.job.Job",
        "class": "CondorJob"
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
class CondorJobService (saga.adaptors.cpi.job.Service):
    """ implements saga.adaptors.cpi.job.Service
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._cpi_base = super(CondorJobService, self)
        self._cpi_base.__init__(api, adaptor)

    # ----------------------------------------------------------------
    #
    def __del__(self):

        # FIXME: not sure if we should PURGE here -- that removes states which
        # might not be evaluated, yet.  Should we mark state evaluation
        # separately?
        #   cmd_state () { touch $DIR/purgeable; ... }
        # When should that be done?
        #self._logger.error("adaptor dying... %s" % self.njobs)
        #self._logger.trace()

        self.finalize(kill_shell=True)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm            = rm_url
        self.session       = session
        self.ppn           = 0
        self.is_cray       = False
        self.jobs          = dict()
        self.query_options = dict()

        rm_scheme = rm_url.scheme
        pty_url   = deepcopy(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query is not None:
            for key, val in parse_qs(rm_url.query).iteritems():
                self.query_options[key] = val[0]

        # we need to extrac the scheme for PTYShell. That's basically the
        # job.Serivce Url withou the condor+ part. We use the PTYShell to execute
        # condor commands either locally or via gsissh or ssh.
        if rm_scheme == "condor":
            pty_url.scheme = "fork"
        elif rm_scheme == "condor+ssh":
            pty_url.scheme = "ssh"
        elif rm_scheme == "condor+gsissh":
            pty_url.scheme = "gsissh"

        # these are the commands that we need in order to interact with PBS.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note avaialbe.
        self._commands = {'condor_version': None,
                          'condor_submit':  None,
                          'condor_q':       None,
                          'condor_rm':      None}

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
        # check if all required condor tools are available
        for cmd in self._commands.keys():
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding Condor tools: %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
            else:
                path = out.strip()  # strip removes newline
                if cmd == 'condor_version':
                    ret, out, _ = self.shell.run_sync("%s" % cmd)
                    if ret != 0:
                        message = "Error determining Condor version: %s" % out
                        log_error_and_raise(message, saga.NoSuccess,
                            self._logger)
                    else:
                        # version is reported as:
                        # $CondorVersion: 7.8.6 Oct 25 2012 $
                        # $CondorPlatform: X86_64-CentOS_5.7 $
                        lines = out.split('\n')
                        version = lines[0].replace("$CondorVersion: ", "")
                        version = version.strip(" $")

                        # add path and version to the command dictionary
                self._commands[cmd] = {"path":    path,
                                       "version": version}

        self._logger.info("Found Condor tools: %s" % self._commands)


    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        pass

    # ----------------------------------------------------------------
    #
    def _job_run(self, jd):
        """ runs a job via qsub
        """
        if (self.queue is not None) and (jd.queue is not None):
            self._logger.warning("Job service was instantiated explicitly with \
'queue=%s', but job description tries to a differnt queue: '%s'. Using '%s'." \
                % (self.queue, jd.queue, self.queue))

        # create a PBS job script from SAGA job description
        script = _condorcript_generator(url=self.rm, logger=self._logger, jd=jd,
            ppn=self.ppn, is_cray=self.is_cray, queue=self.queue)
        self._logger.debug("Generated PBS script: %s" % script)

        ret, out, _ = self.shell.run_sync("echo '%s' | %s" \
            % (script, self._commands['qsub']['path']))

        if ret != 0:
            # something went wrong
            message = "Error running job via 'qsub': %s. Script was: %s" \
                % (out, script)
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # stdout contains the job id
            job_id = "[%s]-[%s]" % (self.rm, out.strip().split('.')[0])
            self._logger.info("Submitted PBS job with id: %s" % job_id)

            # add job to internal list of known jobs.
            self.jobs[job_id] = {
                'state':        saga.job.PENDING,
                'exec_hosts':   None,
                'returncode':   None,
                'create_time':  None,
                'start_time':   None,
                'end_time':     None,
                'gone':         False
            }

            return job_id

    # ----------------------------------------------------------------
    #
    def _retrieve_job(self, job_id):
        """ see if we can get some info about a job that we don't
            know anything about
        """
        rm, pid = self._adaptor.parse_id(job_id)

        # run the PBS 'qstat' command to get some infos about our job
        ret, out, _ = self.shell.run_sync("%s -f1 %s | \
            egrep '(job_state)|(exec_host)|(exit_status)|(ctime)|(start_time)|(comp_time)'" % (self._commands['qstat']['path'], pid))

        if ret != 0:
            message = "Couldn't reconnect to job '%s': %s" % (job_id, out)
            log_error_and_raise(message, saga.NoSuccess, self._logger)

        else:
            # the job seems to exist on the backend. let's gather some data
            job_info = {
                'state':        saga.job.UNKNOWN,
                'exec_hosts':   None,
                'returncode':   None,
                'create_time':  None,
                'start_time':   None,
                'end_time':     None,
                'gone':         False
            }

            results = out.split('\n')
            for line in results:
                if len(line.split('=')) == 2:
                    key, val = line.split('=')
                    key = key.strip()  # strip() removes whitespaces at the
                    val = val.strip()  # beginning and the end of the string

                    if key == 'job_state':
                        job_info['state'] = _condor_to_saga_jobstate(val)
                    elif key == 'exec_host':
                        job_info['exec_hosts'] = val.split('+')
                    elif key == 'exit_status':
                        job_info['returncode'] = val
                    elif key == 'ctime':
                        job_info['create_time'] = val
                    elif key == 'start_time':
                        job_info['start_time'] = val
                    elif key == 'comp_time':
                        job_info['end_time'] = val

            return job_info

    # ----------------------------------------------------------------
    #
    def _job_get_info(self, job_id):
        """ get job attributes via qstat
        """

        # if we don't have the job in our dictionary, we don't want it
        if job_id not in self.jobs:
            message = "Unkown job ID: %s. Can't update state." % job_id
            log_error_and_raise(message, saga.NoSuccess, self._logger)

        # prev. info contains the info collect when _job_get_info
        # was called the last time
        prev_info = self.jobs[job_id]

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever
        if prev_info['gone'] is True:
            self._logger.warning("Job information is not available anymore.")
            return prev_info

        # curr. info will contain the new job info collect. it starts off
        # as a copy of prev_info
        curr_info = deepcopy(prev_info)

        rm, pid = self._adaptor.parse_id(job_id)

        # run the PBS 'qstat' command to get some infos about our job
        ret, out, _ = self.shell.run_sync("%s -f1 %s | \
            egrep '(job_state)|(exec_host)|(exit_status)|(ctime)|(start_time)|(comp_time)'" % (self._commands['qstat']['path'], pid))

        if ret != 0:
            if ("Unknown Job Id" in out):
                # Let's see if the previous job state was runnig or pending. in
                # that case, the job is gone now, which can either mean DONE,
                # or FAILED. the only thing we can do is set it to 'DONE'
                if prev_info['state'] in [saga.job.RUNNING, saga.job.PENDING]:
                    curr_info['state'] = saga.job.DONE
                    curr_info['gone'] = True
                    self._logger.warning("Previously running job has \
disappeared. This probably means that the backend doesn't store informations \
about finished jobs. Setting state to 'DONE'.")
                else:
                    curr_info['gone'] = True
            else:
                # something went wrong
                message = "Error retrieving job info via 'qstat': %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # parse the egrep result. this should look something like this:
            #     job_state = C
            #     exec_host = i72/0
            #     exit_status = 0
            results = out.split('\n')
            for result in results:
                if len(result.split('=')) == 2:
                    key, val = result.split('=')
                    key = key.strip()  # strip() removes whitespaces at the
                    val = val.strip()  # beginning and the end of the string

                    if key == 'job_state':
                        curr_info['state'] = _condor_to_saga_jobstate(val)
                    elif key == 'exec_host':
                        curr_info['exec_hosts'] = val.split('+')  # format i73/7+i73/6+...
                    elif key == 'exit_status':
                        curr_info['returncode'] = val
                    elif key == 'ctime':
                        curr_info['create_time'] = val
                    elif key == 'start_time':
                        curr_info['start_time'] = val
                    elif key == 'comp_time':
                        curr_info['end_time'] = val

        # return the new job info dict
        return curr_info

    # ----------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        """ get the job's state
        """
        # check if we have already reach a terminal state
        if self.jobs[job_id]['state'] == saga.job.CANCELED \
        or self.jobs[job_id]['state'] == saga.job.FAILED \
        or self.jobs[job_id]['state'] == saga.job.DONE:
            return self.jobs[job_id]['state']

        # check if we can / should update
        if (self.jobs[job_id]['gone'] is not True):
            self.jobs[job_id] = self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['state']

    # ----------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_id):
        """ get the job's exit code
        """
        # check if we can / should update
        if (self.jobs[job_id]['gone'] is not True) \
        and (self.jobs[job_id]['returncode'] is None):
            self.jobs[job_id] = self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['returncode']

    # ----------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_id):
        """ get the job's exit code
        """
        # check if we can / should update
        if (self.jobs[job_id]['gone'] is not True) \
        and (self.jobs[job_id]['exec_hosts'] is None):
            self.jobs[job_id] = self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['exec_hosts']

    # ----------------------------------------------------------------
    #
    def _job_get_create_time(self, job_id):
        """ get the job's creation time
        """
        # check if we can / should update
        if (self.jobs[job_id]['gone'] is not True) \
        and (self.jobs[job_id]['create_time'] is None):
            self.jobs[job_id] = self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['create_time']

    # ----------------------------------------------------------------
    #
    def _job_get_start_time(self, job_id):
        """ get the job's start time
        """
        # check if we can / should update
        if (self.jobs[job_id]['gone'] is not True) \
        and (self.jobs[job_id]['start_time'] is None):
            self.jobs[job_id] = self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['start_time']

    # ----------------------------------------------------------------
    #
    def _job_get_end_time(self, job_id):
        """ get the job's end time
        """
        # check if we can / should update
        if (self.jobs[job_id]['gone'] is not True) \
        and (self.jobs[job_id]['end_time'] is None):
            self.jobs[job_id] = self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['end_time']

    # ----------------------------------------------------------------
    #
    def _job_cancel(self, job_id):
        """ cancel the job via 'qdel'
        """
        rm, pid = self._adaptor.parse_id(job_id)

        ret, out, _ = self.shell.run_sync("%s %s\n" \
            % (self._commands['qdel']['path'], pid))

        if ret != 0:
            message = "Error canceling job via 'qdel': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)

        # assume the job was succesfully canceld
        self.jobs[job_id]['state'] = saga.job.CANCELED

    # ----------------------------------------------------------------
    #
    def _job_wait(self, job_id, timeout):
        """ wait for the job to finish or fail
        """

        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(job_id)

        while True:
            state = self._job_get_state(job_id=job_id)

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
                         "job_schema":      self.rm.schema,
                         "reconnect":       False
                        }

        return saga.job.Job(_adaptor=self._adaptor,
                            _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, jobid):
        """ Implements saga.adaptors.cpi.job.Service.get_job()
        """

        # try to get some information about this job and throw it into
        # our job dictionary.
        self.jobs[jobid] = self._retrieve_job(jobid)

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         # TODO: fill job description
                         "job_description": saga.job.Description(),
                         "job_schema":      self.rm.schema,
                         "reconnect":       True,
                         "reconnect_jobid": jobid
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

        ret, out, _ = self.shell.run_sync("%s -l | grep `whoami`"\
            % self._commands['qstat']['path'])

        if ret != 0 and len(out) > 0:
            message = "failed to list jobs via 'qstat': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        elif ret != 0 and len(out) == 0:
            # qstat | grep `` exits with 1 if the list is empty
            pass
        else:
            for line in out.split("\n"):
                # output looks like this:
                # 112059.svc.uc.futuregrid testjob oweidner 0 Q batch
                # 112061.svc.uc.futuregrid testjob oweidner 0 Q batch
                if len(line.split()) > 1:
                    jobid = "[%s]-[%s]" % (self.rm, line.split()[0].split('.')[0])
                    ids.append(str(jobid))

        return ids


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
class CondorJob (saga.adaptors.cpi.job.Job):
    """ implements saga.adaptors.cpi.job.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        self._cpi_base = super(CondorJob, self)
        self._cpi_base.__init__(api, adaptor)

    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements saga.adaptors.cpi.job.Job.init_instance()
        """
        # init_instance is called for every new saga.job.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        if job_info['reconnect'] is True:
            self._id = job_info['reconnect_jobid']
            self._started = True
        else:
            self._id = None
            self._started = False

        return self.get_api()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ mplements saga.adaptors.cpi.job.Job.get_state()
        """
        if self._started is False:
            # jobs that are not started are always in 'NEW' state
            return saga.job.NEW
        else:
            return self.js._job_get_state(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements saga.adaptors.cpi.job.Job.wait()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                saga.IncorrectState, self._logger)
        else:
            self.js._job_wait(self._id, timeout)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements saga.adaptors.cpi.job.Job.cancel()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                saga.IncorrectState, self._logger)
        else:
            self.js._job_cancel(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements saga.adaptors.cpi.job.Job.run()
        """
        self._id = self.js._job_run(self.jd)
        self._started = True

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """ implements saga.adaptors.cpi.job.Job.get_service_url()
        """
        return self.js.rm

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
        if self._started is False:
            return None
        else:
            return self.js._job_get_exit_code(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """ implements saga.adaptors.cpi.job.Job.get_created()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_create_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """ implements saga.adaptors.cpi.job.Job.get_started()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_start_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """ implements saga.adaptors.cpi.job.Job.get_finished()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_end_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """ implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_execution_hosts(self._id)

