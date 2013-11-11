
__author__    = "Andre Merzky, Christian Pérez-Llamas, Ole Weidner, Thomas Schatz"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" SGE job adaptor implementation
"""

import saga.utils.pty_shell

import saga.adaptors.base
import saga.adaptors.cpi.job

from saga.job.constants import *

import os
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
# simple parser for getting memory requirements flags and multipliers from the memreqs part of the job.Service url
#
def _parse_memreqs(s):
    flags = []
    multipliers = []
    while not s=='':
        # find multiplier
        m = re.match(r'\d+\.?\d*|\d*\.?\d+', s)
        if m:
            multipliers.append(float(s[m.start():m.end()]))
            s = s[m.end():]
        else:
            multipliers.append(1.)        
        # find flag
        pos = s.find('~')
        if pos < 0:
            flags.append(s)
            s = ''
        else:                
            flags.append(s[:pos])
            s = s[pos+1:]
    return flags, multipliers

# --------------------------------------------------------------------
#
#
def _sgescript_generator(url, logger, jd, pe_list, queue=None, memreqs=None):
    """ generates an SGE script from a SAGA job description
    """
    sge_params = str()
    exec_n_args = str()

    # if no cores are requested at all, we default to one
    if jd.total_cpu_count is None:
        jd.total_cpu_count = 1

    # check spmd variation. this translates to the SGE qsub -pe flag.
    if jd.spmd_variation is not None:
        if jd.spmd_variation not in pe_list:
            raise Exception("'%s' is not a valid option for jd.spmd_variation. \
Valid options are: %s" % (jd.spmd_variation, pe_list))
    else:
        if jd.total_cpu_count > 1:
            raise Exception("jd.spmd_variation need to be set in order for jd.total_cpu_count to be greater than 1. Valid options for jd.spmd_variation are: %s" % (pe_list))

    if jd.executable is not None:
        exec_n_args += "%s " % (jd.executable)
    if jd.arguments is not None:
        for arg in jd.arguments:
            exec_n_args += "%s " % (arg)

    if jd.name is not None:
        sge_params += "#$ -N %s \n" % jd.name

    sge_params += "#$ -V \n"

    if jd.environment is not None and len(jd.environment) > 0:
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

    if (jd.queue is not None) and (queue is not None):
        sge_params += "#$ -q %s \n" % queue
    elif (jd.queue is not None) and (queue is None):
        sge_params += "#$ -q %s \n" % jd.queue
    elif (jd.queue is None) and (queue is not None):
        sge_params += "#$ -q %s \n" % queue
    else:
        raise Exception("No queue defined.")

    if jd.project is not None:
        sge_params += "#$ -A %s \n" % str(jd.project)
    if jd.job_contact is not None:
        sge_params += "#$ -m be \n"
        sge_params += "#$ -M %s \n" % jd.contact

    # memory requirements - TOTAL_PHYSICAL_MEMORY
    # it is assumed that the value passed through jd is always in Megabyte
    if jd.total_physical_memory is not None:
        # this is (of course) not the same for all SGE installations. some 
        # use virtual_free, some use a combination of mem_req / h_vmem. 
        # It is very annoying. We need some sort of configuration variable 
        # that can control this. Yes, ugly and not very saga-ish, but 
        # the only way to do this, IMHO...
        if memreqs is None:
            raise Exception("When using 'total_physical_memory' with the SGE adaptor, the query parameters of the job.Service URL must define the attributes used by your particular instance of SGE to control memory allocation.\n 'virtual_free', 'h_vmem' or 'mem_req' are commonly encountered examples of such attributes.\n A valid job.Service URL could be for instance:\n 'sge+ssh://myserver.edu?memreqs=virtual_free~1.5h_vmem'\n here the attribute 'virtual_free' would be set to 'total_physical_memory' and the attribute 'h_vmem' would be set to 1.5*'total_physical_memory', '~' is used as a separator.")            
        
        flags, multipliers = _parse_memreqs(memreqs)
        for flag,mult in zip(flags, multipliers):
            sge_params += "#$ -l %s=%sm \n" % (flag, int (round (mult*int(jd.total_physical_memory) ) ) )

    # if no cores are requested at all, we default to one
    if jd.total_cpu_count is None:
        jd.total_cpu_count = 1

    # we need to translate the # cores requested into
    # multiplicity, i.e., if one core is requested and
    # the cluster consists of 16-way SMP nodes, we will
    # request 16. If 17 cores are requested, we will
    # request 32... and so on ... self.__ppn represents
    # the core count per single node
    #count = int(int(jd.total_cpu_count) / int(ppn))
    #if int(jd.total_cpu_count) % int(ppn) != 0:
    #    count = count + 1
    #count = count * int(ppn)

    # escape all double quotes and dollarsigns, otherwise 'echo |'
    # further down won't work
    # only escape '$' in args and exe. not in the params
    exec_n_args = exec_n_args.replace('$', '\\$')

    if jd.spmd_variation is not None:
        sge_params += "#$ -pe %s %s" % (jd.spmd_variation, jd.total_cpu_count)       

    sgescript = "\n#!/bin/bash \n%s \n%s" % (sge_params, exec_n_args)

    sgescript = sgescript.replace('"', '\\"')
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
                          saga.job.SPMD_VARIATION,
                          saga.job.TOTAL_CPU_COUNT,
                          saga.job.TOTAL_PHYSICAL_MEMORY],
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
The SGE (Sun/Oracle Grid Engine) adaptor allows to run and manage jobs on
`SGE <http://en.wikipedia.org/wiki/Oracle_Grid_Engine>`_ controlled HPC clusters.
""",
    "example": "examples/jobs/sgejob.py",
    "schemas": {"sge":        "connect to a local cluster",
                "sge+ssh":    "conenct to a remote cluster via SSH",
                "sge+gsissh": "connect to a remote cluster via GSISSH"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {
    "name"        :    _ADAPTOR_NAME,
    "version"     : "v0.1",
    "schemas"     : _ADAPTOR_SCHEMAS,
    "capabilities":  _ADAPTOR_CAPABILITIES,
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
class Adaptor (saga.adaptors.base.Base):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by 
        the SAGA engine), and which registers the CPI implementation classes 
        which provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self):

        saga.adaptors.base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.opts  = self.get_config (_ADAPTOR_NAME)

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

        _cpi_base = super(SGEJobService, self)
        _cpi_base.__init__(api, adaptor)

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
        self.pe_list = list()
        self.jobs    = dict()
        self.queue   = None
        self.memreqs = None
        self.shell   = None
        self.mandatory_memreqs = list()



        rm_scheme = rm_url.scheme
        pty_url   = deepcopy(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query is not None:
            for key, val in parse_qs(rm_url.query).iteritems():
                if key == 'queue':
                    self.queue = val[0]
                elif key == 'memreqs':
                    self.memreqs = val[0]

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

        self.shell = saga.utils.pty_shell.PTYShell(pty_url, self.session)

      # self.shell.set_initialize_hook(self.initialize)
      # self.shell.set_finalize_hook(self.finalize)

        self.initialize()

        return self.get_api ()


    # ----------------------------------------------------------------
    #
    def close (self) :
        if  self.shell :
            self.shell.finalize (True)


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
                    # fix for a bug in certain qstat versions that return
                    # '1' after a successfull qstat -help:
                    # https://github.com/saga-project/saga-python/issues/163
                    if cmd == 'qstat':
                        version = out.strip().split('\n')[0]
                    else:
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

        # determine the available processing elements
        ret, out, _ = self.shell.run_sync('%s -spl' %
                      (self._commands['qconf']['path']))
        if ret != 0:
            message = "Error running 'qconf': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            for pe in out.split('\n'):
                if pe != '':
                    self.pe_list.append(pe)
            self._logger.debug("Available processing elements: %s" %
                (self.pe_list))
         
        # find out mandatory and optional memory attributes 
        ret, out, _ = self.shell.run_sync('%s -sc' % (self._commands['qconf']['path']))
        if ret != 0:
            message = "Error running 'qconf': %s" % out
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else: 
            mandatory_attrs = []
            optional_attrs = []           
            for line in out.split('\n'):
                if (line != '') and (line[0] != '#'):
                    [name, _, att_type, _, requestable, _, _, _] = line.split()
                    if att_type == 'MEMORY' and requestable == 'YES':
                        optional_attrs.append(name)
                    elif att_type == 'MEMORY' and requestable == 'FORCED':
                        mandatory_attrs.append(name)
            self._logger.debug("Optional memory attributes: %s" % (mandatory_attrs))
            self._logger.debug("Mandatory memory attributes: %s" % (optional_attrs))       
        # find out user specified memory attributes in job.Service URL
        if self.memreqs is None:
            flags = []
        else:
            flags, _ = _parse_memreqs(self.memreqs) 
        # if there are mandatory memory attributes store them and check that they were specified in the job.Service URL
        if not (mandatory_attrs == []):
            self.mandatory_memreqs = mandatory_attrs
            missing_flags = []
            for attr in mandatory_attrs:
                if not attr in flags:
                    missing_flags.append(attr)
            if not (missing_flags == []):
                message = "The following memory attribute(s) are mandatory in your SGE environment and thus must be specified in the job service URL: %s" % ' '.join(missing_flags)
                log_error_and_raise(message, saga.BadParameter, self._logger) 
        # if memory attributes were specified in the job.Service URL, check that they correspond to existing optional or mandatory memory attributes
        invalid_attrs = []              
        for f in flags:
            if not (f in optional_attrs or f in mandatory_attrs):
                invalid_attrs.append(f)
        if not (invalid_attrs == []):
            message = "The following memory attribute(s) were specified in the job.Service URL but are not valid memory attributes in your SGE environment: %s" % ' '.join(invalid_attrs)
            log_error_and_raise(message, saga.BadParameter, self._logger)
                    

    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)

    def _remote_mkdir(self, path):
        # check if the path exists
        ret, out, _ = self.shell.run_sync(
                        "(test -d %s && echo -n 0) || (mkdir -p %s && echo -n 1)" % (path, path))

        if ret == 0 and out == "1":
            self._logger.info("Remote directory created: %s" % path)
        elif ret != 0:
            # something went wrong
            message = "Couldn't create remote directory - %s" % (out)
            log_error_and_raise(message, saga.NoSuccess, self._logger)

    # ----------------------------------------------------------------
    #
    def _job_run(self, jd):
        """ runs a job via qsub
        """
        if (self.queue is not None) and (jd.queue is not None):
            self._logger.warning("Job service was instantiated explicitly with \
'queue=%s', but job description tries to a differnt queue: '%s'. Using '%s'." %
                (self.queue, jd.queue, self.queue))
        # In SGE environments with mandatory memory attributes, 'total_physical_memory' must be specified        
        if (not (self.mandatory_memreqs == [])) and (jd.total_physical_memory is None):
            log_error_and_raise("Your SGE environments has mandatory memory attributes, so 'total_physical_memory' must be specified in your job descriptor", saga.BadParameter, self._logger)            
        try:
            # create a SGE job script from SAGA job description
            script = _sgescript_generator(url=self.rm, logger=self._logger,
                                          jd=jd, pe_list=self.pe_list,
                                          queue=self.queue,
                                          memreqs=self.memreqs)

            self._logger.info("Generated SGE script: %s" % script)
        except Exception, ex:
            log_error_and_raise(str(ex), saga.BadParameter, self._logger)

        # try to create the working directory (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #           compute nodes.
        if jd.working_directory is not None and len(jd.working_directory) > 0:
            self._remote_mkdir(jd.working_directory)

        if jd.output is not None and len(jd.output) > 0:
            self._remote_mkdir(os.path.dirname(jd.output))

        if jd.error is not None and len(jd.error) > 0:
            self._remote_mkdir(os.path.dirname(jd.output))

        # submit the SGE script
        # Now we want to execute the script. This process consists of two steps:
        # (1) we create a temporary file with 'mktemp' and write the contents of 
        #     the generated PBS script into it
        # (2) we call 'qsub <tmpfile>' to submit the script to the queueing system
        cmdline = """SCRIPTFILE=`mktemp -t SAGA-Python-SGEJobScript.XXXXXX` &&  echo "%s" > $SCRIPTFILE && %s $SCRIPTFILE""" %  (script, self._commands['qsub']['path'])
        #cmdline = """echo "%s" | %s""" % (script, self._commands['qsub']['path'])
        ret, out, _ = self.shell.run_sync(cmdline)

        if ret != 0:
            # something went wrong
            message = "Error running job via 'qsub': %s. Commandline was: %s" \
                % (out, cmdline)
            log_error_and_raise(message, saga.NoSuccess, self._logger)
        else:
            # stdout contains the job id:
            # Your job 1036608 ("testjob") has been submitted
            pid = None
            for line in out.split('\n'):
                if line.find("Your job") != -1:
                    pid = line.split()[2]
            if pid is None:
                message = "Couldn't parse job id from 'qsub' output: %s" % out
                log_error_and_raise(message, saga.NoSuccess, self._logger)

            job_id = "[%s]-[%s]" % (self.rm, pid)
            self._logger.info("Submitted SGE job with id: %s" % job_id)

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

        # run the SGE 'qstat' command to get some infos about our job
        ret, out, _ = self.shell.run_sync("echo -n \"JID: \" && %s | grep %s \
                && %s -f -j %s | egrep '(submission_time)'" \
                % (self._commands['qstat']['path'], pid,
                   self._commands['qstat']['path'], pid))

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
            for result in results:
                if 'JID:' in result:
                    job_state = _sge_to_saga_jobstate(out.split()[5])
                    job_info['state'] = job_state
                elif 'submission_time:' in result:
                    val = result.replace('submission_time:', '')
                    job_info['create_time'] = val.strip()

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

        # run the SGE 'qstat' command to get some infos about our job
        ret, out, _ = self.shell.run_sync("echo -n \"JID: \" && %s | grep %s \
                && %s -f -j %s | egrep '(submission_time)'" \
                % (self._commands['qstat']['path'], pid,
                   self._commands['qstat']['path'], pid))

        if ret != 0:
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
            results = out.split('\n')
            for result in results:
                if 'JID:' in result:
                    job_state = _sge_to_saga_jobstate(out.split()[5])
                    curr_info['state'] = job_state
                elif 'submission_time:' in result:
                    val = result.replace('submission_time:', '')
                    curr_info['create_time'] = val.strip()

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

        ret = self.jobs[job_id]['returncode']

        # FIXME: 'None' should cause an exception
        if ret == None : return None
        else           : return int(ret)

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

            if state == saga.job.UNKNOWN :
                log_error_and_raise("cannot get job state", saga.IncorrectState, self._logger)

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

        ret, out, _ = self.shell.run_sync("%s | grep `whoami`"\
            % self._commands['qstat']['path'])
        if ret != 0 and len(out) > 0:
            message = "Failed to list jobs via 'qstat': %s" % out
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
        _cpi_base = super(SGEJob, self)
        _cpi_base.__init__(api, adaptor)

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

