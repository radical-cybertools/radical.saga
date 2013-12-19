
__author__    = "Hangi,Kim"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" IBM LoadLeveler job adaptor implementation
    reference for pbs job adaptor implementation
	Hangi, Kim hgkim@kisti.re.kr
"""

import radical.utils.which
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
def _ll_to_saga_jobstate(lljs):
    """ translates a loadleveler one-letter state to saga
        pbs_loadl_comparison.xlsx
    """
    if lljs == 'C':
        return saga.job.DONE
    elif lljs == 'S':
        return saga.job.PENDING
    elif lljs == 'ST':
        return saga.job.PENDING
    elif lljs == 'I':
        return saga.job.PENDING
    elif lljs == 'R':
        return saga.job.RUNNING
    else:
        return saga.job.UNKNOWN


# --------------------------------------------------------------------
#
def _loadlcript_generator(url, logger, jd, ppn, queue=None):
    """ generates a IMB LoadLeveler script from a SAGA job description
    """
    loadl_params = str()
    exec_n_args = str()

    if jd.executable is not None:
        exec_n_args += "%s " % (jd.executable)
    if jd.arguments is not None:
        for arg in jd.arguments:
            exec_n_args += "%s " % (arg)

    if jd.name is not None:
        loadl_params += "#@job_name=%s \n" % jd.name

    if jd.environment is not None:
        variable_list = str()
        for key in jd.environment.keys():
            variable_list += "%s=%s;" % (key, jd.environment[key])
        loadl_params += "#@environment=%s \n" % variable_list

    if jd.working_directory is not None:
        loadl_params += "#@initialdir=%s \n" % jd.working_directory
    if jd.output is not None:
        loadl_params += "#@output=%s \n" % jd.output
    if jd.error is not None:
        loadl_params += "#@error=%s \n" % jd.error
    if jd.wall_time_limit is not None:
        hours = jd.wall_time_limit / 60
        minutes = jd.wall_time_limit % 60
        loadl_params += "#@wall_clock_limit=%s:%s:00 \n" \
            % (str(hours), str(minutes))

    if jd.total_cpu_count is None:
        # try to come up with a sensible (?) default value
        jd.total_cpu_count = 1

    if jd.total_physical_memory is not None:
        # try to come up with a sensible (?) default value for memeory
        jd.total_physical_memory = 256

    loadl_params += "#@resources=ConsumableCpus(%s)ConsumableMemory(%smb)\n" % \
        (jd.total_cpu_count, jd.total_physical_memory)

    if jd.job_contact is not None:
        loadl_params += "#@notify_user=%s\n" % jd.job_contact

    # some default (?) parameter that seem to work fine everywhere... 
    loadl_params += "#@class=normal\n"
    loadl_params += "#@notification=complete\n"

    # finally, we 'queue' the job
    loadl_params += "#@queue\n"

    loadlscript = "\n#!/bin/bash \n%s%s" % (loadl_params, exec_n_args)
	# add by hgkim 
    logger.info(loadlscript)
    return loadlscript


def getId(out):
    jobId=-1
    CLUSTERFINDWORDS="has been submitted to cluster"

    t=out.split('\n')

    for line in t:
        if line.startswith('Job') and jobId==-1:
            tmpStr=line.split(' ')
            jobId=tmpStr[1]

        if line.find(CLUSTERFINDWORDS)!=-1:
            #print "find:", line
            tmpStr2=line.split(CLUSTERFINDWORDS)
            tmp=tmpStr2[1].strip()
            tmpLen=len(tmp)
            clusterId=tmp[1:tmpLen-1]

    return jobId


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.loadljob"
_ADAPTOR_SCHEMAS       = ["loadl", "loadl+ssh", "loadl+gsissh"]
_ADAPTOR_OPTIONS       = []

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
    "description":  """
The LoadLeveler adaptor allows to run and manage jobs on ` IBM LoadLeveler<http://www-03.ibm.com/systems/software/loadleveler/>`_
controlled HPC clusters.
""",
    "example": "examples/jobs/loadljob.py",
    "schemas": {"loadl":        "connect to a local cluster",
                "loadl+ssh":    "conenct to a remote cluster via SSH",
                "loadl+gsissh": "connect to a remote cluster via GSISSH"}
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
        "class": "LOADLJobService"
        },
        {
        "type": "saga.job.Job",
        "class": "LOADLJob"
        }
    ]
}


###############################################################################
# The adaptor class
#class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
class Adaptor (saga.adaptors.base.Base):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by 
        the SAGA engine), and which registers the CPI implementation classes 
        which provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self):

        saga.adaptors.base.Base.__init__(self,
            _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.opts = self.get_config(_ADAPTOR_NAME)

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
class LOADLJobService (saga.adaptors.cpi.job.Service):
    """ implements saga.adaptors.cpi.job.Service
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._cpi_base = super(LOADLJobService, self)
        self._cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor

    # ----------------------------------------------------------------
    #
    def __del__(self):

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
        self.queue   = None
        self.jobs    = dict()
        self.query_options = dict()
        self.cluster = None

        rm_scheme = rm_url.scheme
        pty_url   = deepcopy(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query is not None:
            for key, val in parse_qs(rm_url.query).iteritems():
                if key == 'queue':
                    self.queue = val[0]
                if key == 'cluster':
                    self.cluster= val[0]

        # we need to extrac the scheme for PTYShell. That's basically the
        # job.Serivce Url withou the pbs+ part. We use the PTYShell to execute
        # pbs commands either locally or via gsissh or ssh.
        if rm_scheme == "loadl":
            pty_url.scheme = "fork"
        elif rm_scheme == "loadl+ssh":
            pty_url.scheme = "ssh"
        elif rm_scheme == "loadl+gsissh":
            pty_url.scheme = "gsissh"

        # these are the commands that we need in order to interact with PBS.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note avaialbe.
        self._commands = {'llq': None,
                          'llsubmit':     None,
                          'llcancel':     None}

        self.shell = saga.utils.pty_shell.PTYShell(pty_url, self.session)

        #self.shell.set_initialize_hook(self.initialize)
        #self.shell.set_finalize_hook(self.finalize)

        self.initialize()

        return self.get_api ()

    # ----------------------------------------------------------------
    #
    def initialize(self):
        # check if all required pbs tools are available
        for cmd in self._commands.keys():
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            self._logger.info(ret)
            self._logger.info(out)
            if ret != 0:
                message = "Error finding LoadLeveler tools: %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
            else:
                path = out.strip()  # strip removes newline
                ret, out, _ = self.shell.run_sync("%s -v" % cmd)
                if ret != 0:
                    message = "Error finding LoadLeveler tools: %s" % out
                    log_error_and_raise(message, saga.NoSuccess,
                        self._logger)
                else:
                    # version is reported as: "version: x.y.z"
                    version = out.strip().split()[1]

                    # add path and version to the command dictionary
                    self._commands[cmd] = {"path":    path,
                                           "version": version}

        self._logger.info("Found LoadLeveler tools: %s" % self._commands)

        # see if we can get some information about the cluster, e.g.,
        # different queues, number of processes per node, etc.
        # TODO: this is quite a hack. however, it *seems* to work quite
        #       well in practice.
        # modi by hgkim
        """
        ret, out, _ = self.shell.run_sync('%s -a | grep np' % \
            self._commands['pbsnodes']['path'])
        if ret != 0:
            message = "Error running pbsnodes: %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # this is black magic. we just assume that the highest occurence
            # of a specific np is the number of processors (cores) per compute
            # node. this equals max "PPN" for job scripts
            ppn_list = dict()
            for line in out.split('\n'):
                np = line.split(' = ')
                if len(np) == 2:
                    np = np[1].strip()
                    if np in ppn_list:
                        ppn_list[np] += 1
                    else:
                        ppn_list[np] = 1
			# add by hgkim
            #self.ppn = max(ppn_list, key=ppn_list.get)
            self.ppn = 1
            self._logger.debug("Found the following 'ppn' configurations: %s. \
    Using %s as default ppn." 
                % (ppn_list, self.ppn))
        """

    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)

    # ----------------------------------------------------------------
    #
    def _job_run(self, jd):
        """ runs a job via llsubmit
        """
        if (self.queue is not None) and (jd.queue is not None):
            self._logger.warning("Job service was instantiated explicitly with \
'queue=%s', but job description tries to a differnt queue: '%s'. Using '%s'." %
                (self.queue, jd.queue, self.queue))

        try:
            # create a LoadLeveler job script from SAGA job description
            script = _loadlcript_generator(url=self.rm, logger=self._logger,
                                         jd=jd, ppn=self.ppn,
                                         queue=self.queue)

            # escape all double quotes and dollarsigns, otherwise 'echo |' 
            # further down won't work
            script = script.replace('"', '\\"')
            script = script.replace('$', '\\$')

            self._logger.debug("Generated LoadLeveler script: %s" % script)
        except Exception, ex:
            log_error_and_raise(str(ex), saga.BadParameter, self._logger)

        ret, out, _ = self.shell.run_sync("""echo "%s" | %s -X %s -""" \
            % (script, self._commands['llsubmit']['path'], self.cluster))

        if ret != 0:
            # something went wrong
            message = "Error running job via 'llsubmit': %s. Script was: %s" \
                % (out, script)
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # stdout contains the job id
            #job_id = "[%s]-[%s]" % (self.rm, out.strip().split('.')[0])
            job_id = "[%s]-[%s]" % (self.rm, getId(out))
            self._logger.info("Submitted LoadLeveler job with id: %s" % job_id)

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

        # run the LoadLeveler 'llq' command to get some infos about our job
        #ret, out, _ = self.shell.run_sync("%s -f1 %s | \
        #    egrep '(job_state)|(exec_host)|(exit_status)|(ctime)|\
#(start_time)|(comp_time)'" % (self._commands['qstat']['path'], pid))
        ret, out, _ = self.shell.run_sync("%s -X %s -j %s \
-r %%st %%dd %%cc %%jt %%c %%Xs" % (self._commands['llq']['path'], self.cluster, pid))

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

            lastStr=out.rstrip().split('\n')[-1]
            self._logger.info(lastStr)
            if lastStr.startswith('llq:'): # llq: There is currently no job status to report
                # hgkim 2013/08/22
                #job_info['state'] = _ll_to_saga_jobstate('C')
                job_info['state'] = saga.job.DONE
                job_info['returncode'] = 0
                from datetime import datetime
                job_info['end_time'] = datetime.now().strftime('%c')
            else:
                results = lastStr.split('!')
                self._logger.info("results: %r",results)

                job_info['state'] = _ll_to_saga_jobstate(results[0])
                job_info['returncode'] = -1 # still running
                job_info['start_time'] = results[1]
                #job_info['exec_hosts'] = results[5]

            return job_info

    # ----------------------------------------------------------------
    #
    def _job_get_info(self, job_id):
        """ get job attributes via llq
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

        # run the LoadLeveler 'llq' command to get some infos about our job
        #ret, out, _ = self.shell.run_sync("%s -f1 %s | \
        ret, out, _ = self.shell.run_sync("%s -X %s -j %s \
-r %%st %%dd %%cc %%jt %%c %%Xs" % (self._commands['llq']['path'], self.cluster, pid))

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
                message = "Error retrieving job info via 'llq': %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # parse the result(last string). this should look something like this:
            """ 
            ===== Cluster kisti.glory =====

            06/19 09:51:45 llq: 2539-457 Cannot gethostbyname for machine: glory118.plsi.or.kr
            R!06/19/2013 09:51!!Serial!normal!kisti.login
            0 : Status
            1 : Dispatch Date
            2 : %cc
            3 : Job type
            4 : class
            5 : Cluster name from where the job was submitted
            """ 
            lastStr=out.rstrip().split('\n')[-1]
            self._logger.info(lastStr)
            if lastStr.startswith('llq:'): # llq: There is currently no job status to report
                curr_info['state'] = _ll_to_saga_jobstate('C')
                
                from datetime import datetime
                curr_info['end_time'] = datetime.now().strftime('%c')
            else:
                results = lastStr.split('!')
                self._logger.info(results)

                
                curr_info['state'] = _ll_to_saga_jobstate(results[0])
                curr_info['returncode'] = 0 # later, fix
                curr_info['create_time'] = results[1]
                curr_info['start_time'] = results[1]
                #curr_info['exec_hosts'] = results[5]

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
        """ cancel the job via 'llcancel'
        """
        rm, pid = self._adaptor.parse_id(job_id)

        ret, out, _ = self.shell.run_sync("%s -X %s %s\n" \
            % (self._commands['llcancel']['path'], self.cluster, pid))

        if ret != 0:
            message = "Error canceling job via 'llcancel': %s" % out
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

        self._logger.info("get_job: %r", jobid)
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

        ret, out, _ = self.shell.run_sync("%s | grep `whoami`" %
                                          self._commands['qstat']['path'])

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
class LOADLJob (saga.adaptors.cpi.job.Job):
    """ implements saga.adaptors.cpi.job.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        self._cpi_base = super(LOADLJob, self)
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

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
