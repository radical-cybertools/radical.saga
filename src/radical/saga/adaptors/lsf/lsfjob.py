
__author__    = "The RADICAL Team"
__copyright__ = "Copyright 2012-2019, The SAGA Project"
__license__   = "MIT"


""" LSF job adaptor implementation
"""

import re
import os
import copy
import time
import threading
import datetime

from urllib.parse import parse_qs

import radical.utils  as ru

from ..    import base
from ..    import cpi as cpi_base
from ..cpi import job as cpi

from ...              import exceptions as rse
from ...              import job        as rsj
from ...utils         import pty_shell  as rsups


SYNC_CALL  = cpi_base.decorators.SYNC_CALL
ASYNC_CALL = cpi_base.decorators.ASYNC_CALL

SYNC_WAIT_UPDATE_INTERVAL = 1  # seconds
MONITOR_UPDATE_INTERVAL   = 3  # seconds

# Intel LSF hosts have SMT default to 4
SMT_DEFAULT = 1
SMT_VALID_VALUES = [1, 2, 4]

# FIXME: will be taken from resource config
RESOURCES = {
    'summit': {'cpn': 42,
               'gpn': 6,
               'valid_alloc_flags': [
                   'gpumps',
                   'gpudefault',
                   'nvme',
                   'spectral',
                   'maximizegpfs'
               ]},
    'lassen': {'cpn': 40,
               'gpn': 4,
               'valid_alloc_flags': [
                   'atsdisable',
                   'autonumaoff',
                   'cpublink',
                   'ipisolate'
               ]}
}


# ------------------------------------------------------------------------------
#
class _job_state_monitor(threading.Thread):
    """
    thread that periodically monitors job states
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, job_service):

        self.logger = job_service._logger
        self.js     = job_service
        self._term  = threading.Event()

        super(_job_state_monitor, self).__init__()
        self.setDaemon(True)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self.logger.info('stop  thread for %s', self.js.get_url())
        self._term.set()


    # --------------------------------------------------------------------------
    #
    def run(self):

        self.logger.info('start thread for %s', self.js.get_url())
        while not self._term.isSet():
        
            try:
                # do bulk updates here! we don't want to pull information
                # job by job. that would be too inefficient!
                jobs = self.js.jobs
                for job in jobs:

                    # if the job hasn't been started, we can't update its
                    # state. we can tell if a job has been started if it
                    # has a job id
                    job_info = self.js.jobs[job]
                    if not job_info.get('job_id'):
                        continue

                    # we only need to monitor jobs that are not in a
                    # terminal state, so we can skip the ones that are
                    # either done, failed or canceled
                    state = job_info['state']
                    if state in rsj.FINAL:
                        continue

                    # job is not final and we got new info - replace job info
                    new_info = self.js._job_get_info(job)
                    self.js.jobs[job] = new_info

                    # we only care to state updates though when the state
                    # actually changed from last time
                    new_state = new_info['state']
                    if new_state == state:
                        continue

                    # fire job state callback if 'state' has changed
                    self.logger.info("update Job %s (state: %s)"
                                    % (job, new_state))
                    job._api()._attributes_i_set('state', new_state,
                                                 job._api()._UP, True)

                time.sleep(MONITOR_UPDATE_INTERVAL)

            except Exception as e:
                self.logger.exception("job monitoring thread failed")
                break

        self.logger.info('close thread for %s', self.js.get_url())


# ------------------------------------------------------------------------------
#
def _lsf_to_saga_jobstate(lsfjs):
    """
    translates a lsf one-letter state to saga
    """

    if   lsfjs in ['RUN']                     : return rsj.RUNNING
    elif lsfjs in ['WAIT', 'PEND']            : return rsj.PENDING
    elif lsfjs in ['DONE']                    : return rsj.DONE
    elif lsfjs in ['UNKNOWN', 'ZOMBI', 'EXIT']: return rsj.FAILED
    elif lsfjs in ['USUSP', 'SSUSP', 'PSUSP'] : return rsj.SUSPENDED
    else                                      : return rsj.UNKNOWN


# ------------------------------------------------------------------------------
#
def _lsfscript_generator(url, logger, jd, ppn, lsf_version, queue):
    """
    generates an LSF script from a SAGA job description
    """

    lsf_bsubs   = ''
    command     = ''
    env_string  = ''

    if jd.executable: command += "%s " % (jd.executable)
    if jd.arguments : command += ' '.join(jd.arguments)

    bsub_queue = queue or jd.queue
    if bsub_queue          : lsf_bsubs += "#BSUB -q %s \n" % bsub_queue

    if jd.name             : lsf_bsubs += "#BSUB -J %s \n" % jd.name
    if jd.job_contact      : lsf_bsubs += "#BSUB -u %s \n" % jd.job_contact
    if jd.working_directory: lsf_bsubs += "#BSUB -cwd %s \n" \
                                                     %  jd.working_directory
    if jd.wall_time_limit  : lsf_bsubs += "#BSUB -W %s:%s \n" \
                                                     % (int(jd.wall_time_limit / 60),
                                                        int(jd.wall_time_limit % 60))

    # if working directory is set, we want stdout to end up in the
    # working directory as well, unless it contains a specific
    # path name - otherwise we pass `output` as is.
    if jd.output:
        if os.path.isabs(jd.output): path = ''
        elif jd.working_directory  : path = '%s/' % jd.working_directory
        else                       : path = ''
        lsf_bsubs += "#BSUB -o %s%s \n" % (path, jd.output)

    # same holds for stderr
    if jd.error:
        if os.path.isabs(jd.error): path = ''
        elif jd.working_directory : path = '%s/' % jd.working_directory
        else                      : path = ''
        lsf_bsubs += "#BSUB -e %s%s \n" % (path, jd.error)

    if jd.project and ':' in jd.project:
        account, reservation = jd.project.split(':', 1)
        lsf_bsubs += "#BSUB -P %s \n" % account
        lsf_bsubs += "#BSUB -U %s \n" % reservation

    elif jd.project:
        lsf_bsubs += "#BSUB -P %s \n" % jd.project

    # Request enough nodes to cater for the number of gpus and cores requested
    if not jd.total_cpu_count: total_cpu_count = 1
    else                     : total_cpu_count = jd.total_cpu_count

    if not jd.total_gpu_count: total_gpu_count = 1
    else                     : total_gpu_count = jd.total_gpu_count

    hostname = url.host

    if not hostname or 'localhost' in hostname:
        out, _, ret = ru.sh_callout('hostname -f')
        if ret: hostname = os.environ.get('HOSTNAME', '')
        else  : hostname = out.strip()

    if not hostname:
        raise RuntimeError('cannot determine target host f or %s' % url)

    cpn, gpn, smt, valid_alloc_flags = 0, 1, SMT_DEFAULT, []
    for resource_name in RESOURCES:
        if resource_name in hostname:
            smt = jd.system_architecture.get('smt') or smt
            cpn = RESOURCES[resource_name]['cpn'] * smt
            gpn = RESOURCES[resource_name]['gpn']
            valid_alloc_flags = RESOURCES[resource_name]['valid_alloc_flags']
            break

    if not cpn:
        raise ValueError('LSF host (%s) not yet supported' % hostname)

    if smt not in SMT_VALID_VALUES:
        smt = SMT_DEFAULT

    cpu_nodes = int(total_cpu_count / cpn)
    if total_cpu_count > (cpu_nodes * cpn):
        cpu_nodes += 1

    gpu_nodes = int(total_gpu_count / gpn)
    if total_gpu_count > (gpu_nodes * gpn):
        gpu_nodes += 1

    nodes = max(cpu_nodes, gpu_nodes)
    lsf_bsubs += "#BSUB -nnodes %s \n" % str(nodes)

    alloc_flags = []
    for flag in jd.system_architecture.get('options', []):
        if flag.lower() in valid_alloc_flags:
            alloc_flags.append(flag.lower())
    alloc_flags.append('smt%d' % smt)
    lsf_bsubs += "#BSUB -alloc_flags '%s' \n" % ' '.join(alloc_flags)

    env_string += "export RADICAL_SAGA_SMT=%d" % smt
    if jd.environment:
        for k, v in jd.environment.items():
            env_string += " %s=%s" % (k, v)

    # escape double quotes and dollar signs, otherwise 'echo |'
    # further down won't work
    # only escape '$' in args and exe. not in the bsubs
    command   = command.replace('$', '\\$')
    lsfscript = "\n#!/bin/bash \n%s\n%s\n%s" % (lsf_bsubs, env_string, command)
    lsfscript = lsfscript.replace('"', '\\"')

    return lsfscript


# ------------------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0


# ------------------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME    = "radical.saga.adaptors.lsfjob"
_ADAPTOR_SCHEMAS = ["lsf", "lsf+ssh", "lsf+gsissh"]


# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [rsj.NAME,
                          rsj.EXECUTABLE,
                          rsj.ARGUMENTS,
                          rsj.ENVIRONMENT,
                          rsj.INPUT,
                          rsj.OUTPUT,
                          rsj.ERROR,
                          rsj.QUEUE,
                          rsj.PROJECT,
                          rsj.WALL_TIME_LIMIT,
                          rsj.WORKING_DIRECTORY,
                          rsj.SPMD_VARIATION,
                          rsj.PROCESSES_PER_HOST,
                          rsj.TOTAL_CPU_COUNT,
                          rsj.TOTAL_GPU_COUNT,
                          rsj.SYSTEM_ARCHITECTURE],
    "job_attributes":    [rsj.EXIT_CODE,
                          rsj.EXECUTION_HOSTS,
                          rsj.CREATED,
                          rsj.STARTED,
                          rsj.FINISHED],
    "metrics":           [rsj.STATE],
    "callbacks":         [rsj.STATE],
    "contexts":          {"ssh"      : "SSH public/private keypair",
                          "x509"     : "GSISSH X509 proxy context",
                          "userpass" : "username/password pair (ssh)"}
}

# ------------------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":  """
The LSF adaptor allows to run and manage jobs on
`LSF <https://en.wikipedia.org/wiki/Platform_LSF>`_ controlled HPC clusters.
""",
    "example": "examples/jobs/lsfjob.py",
    "schemas": {"lsf":        "connect to a local cluster",
                "lsf+ssh":    "connect to a remote cluster via SSH",
                "lsf+gsissh": "connect to a remote cluster via GSISSH"}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {"name"        : _ADAPTOR_NAME,
                 "version"     : "v0.2",
                 "schemas"     : _ADAPTOR_SCHEMAS,
                 "capabilities": _ADAPTOR_CAPABILITIES,
                 "cpis": [
                             {
                                 "type": "radical.saga.job.Service",
                                 "class": "LSFJobService"
                             },
                             {
                                 "type": "radical.saga.job.Job",
                                 "class": "LSFJob"
                             }
                         ]
                }


# ------------------------------------------------------------------------------
# The adaptor class
class Adaptor(base.Base):
    '''
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        base.Base.__init__(self, _ADAPTOR_INFO)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime.datetime(1970,1,1)


    # --------------------------------------------------------------------------
    #
    def sanity_check(self):
        # FIXME: also check for gsissh
        pass

    # --------------------------------------------------------------------------
    #
    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if not match or len(match.groups()) != 2:
            raise rse.BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


# ------------------------------------------------------------------------------
#
class LSFJobService(cpi.Service):
    """
    implements cpi.job.Service
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._mt  = None
        _cpi_base = super(LSFJobService, self)
        _cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor

    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if  self.mt:
            self._logger.info("stop   monitoring thread: %s", self.rm)
            self.mt.stop()
            self.mt.join(10)  # don't block forever on join()

        self._logger.info("stopped monitoring thread: %s", self.rm)

        self.finalize(True)


    # --------------------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):

        if  kill_shell:
            if  self.shell:
                self.shell.finalize(True)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm      = rm_url
        self.session = session
        self.ppn     = 1
        self.queue   = None
        self.shell   = None
        self.jobs    = dict()

        # the monitoring thread - one per service instance
        self.mt = _job_state_monitor(job_service=self)
        self.mt.start()

        rm_scheme = rm_url.scheme
        pty_url   = ru.Url(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query:
            for key, val in parse_qs(rm_url.query).items():
                if key == 'queue':
                    self.queue = val[0]
                else:
                    raise rse.BadParameter('unsupported url query %s' % key)

        # we need to extrac the scheme for PTYShell. That's basically the
        # job.Serivce Url withou the lsf+ part. We use the PTYShell to execute
        # lsf commands either locally or via gsissh or ssh.
        elems = rm_scheme.split('+')
        if   'gsissh' in elems: pty_url.scheme = "gsissh"
        elif 'ssh'    in elems: pty_url.scheme = "ssh"
        else                  : pty_url.scheme = "fork"

        # these are the commands that we need in order to interact with LSF.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note avaialbe.
        self._commands = {'bqueues':  dict(),
                          'bjobs':    dict(),
                          'bsub':     dict(),
                          'bkill':    dict()}

        self.shell = rsups.PTYShell(pty_url, self.session)

      # self.shell.set_initialize_hook(self.initialize)
      # self.shell.set_finalize_hook(self.finalize)

        self.initialize()
        return self.get_api()


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # check if all required lsf tools are available
        for cmd in self._commands:

            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                raise rse.NoSuccess("Couldn't find LSF tools: %s" % out)

            else:
                path = out.strip()  # strip removes newline
                ret, out, _ = self.shell.run_sync("%s -V" % cmd)
                if ret != 0:
                    raise rse.NoSuccess("Couldn't find LSF tools: %s" % out)
                else:
                    # version is reported as: "version: x.y.z"
                    version = out.split("\n")[0]

                    # add path and version to the command dictionary
                    self._commands[cmd]["path"]    = path
                    self._commands[cmd]["version"] = version

        self._logger.info("Found LSF tools: %s" % self._commands)

      # # see if we can get some information about the cluster, e.g.,
      # # different queues, number of processes per node, etc.
      # # TODO: this is quite a hack. however, it *seems* to work quite
      # #       well in practice.
      # ret, out, _ = self.shell.run_sync(
      #                   'unset GREP_OPTIONS; %s -a | grep -E "(np|pcpu)"'
      #                   % self._commands['pbsnodes']['path'])
      # if ret != 0:
      #
      #     raise rse.NoSuccess("Error running pbsnodes: %s" % out)
      # else:
      #    # this is black magic. we just assume that the highest occurence
      #    # of a specific np is the number of processors (cores) per compute
      #    # node. this equals max "PPN" for job scripts
      #     ppn_list = dict()
      #     for line in out.split('\n'):
      #         np = line.split(' = ')
      #         if len(np) == 2:
      #             np = np[1].strip()
      #             if np in ppn_list:
      #                 ppn_list[np] += 1
      #             else:
      #                 ppn_list[np] = 1
      #     self.ppn = max(ppn_list, key=ppn_list.get)
      #     self._logger.debug("Found the following 'ppn' configurations: %s. \
      #                         Using %s as default ppn."
      #                       % (ppn_list, self.ppn))


    # --------------------------------------------------------------------------
    #
    def _job_run(self, job_obj):
        """ runs a job via qsub
        """
        # get the job description
        jd = job_obj.jd

        # normalize working directory path
        if  jd.working_directory:
            jd.working_directory = os.path.normpath(jd.working_directory)

        if self.queue and jd.queue:
            self._logger.warning("Job service for queue '%s', \
                                  job goes to queue: '%s'. Using '%s'." %
                                  (self.queue, jd.queue, self.queue))

        try:
            # create an LSF job script from SAGA job description
            script = _lsfscript_generator(url=self.rm, logger=self._logger,
                                jd=jd, ppn=self.ppn,
                                lsf_version=self._commands['bjobs']['version'],
                                queue=self.queue)

            self._logger.info("Generated LSF script: %s" % script)

        except Exception as e:
            raise rse.BadParameter(str(e)) from e

        # try to create the working directory (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #          compute nodes.
        if jd.working_directory:
            pwd = jd.working_directory
            self._logger.info("Creating working directory %s" % pwd)
            ret, out, _ = self.shell.run_sync("mkdir -p %s" % pwd)

            if ret:
                raise rse.NoSuccess("Couldn't create workdir %s" % out)

        # (1) create a temporary file with 'mktemp' and write the contents of
        #     the generated PBS script into it
        # (2) call 'qsub <tmpfile>' to submit the script to the batch system
        #
        cmdline = \
            "SCRIPTFILE=`mktemp -p $HOME -t SAGA-Python-LSFJobScript.XXXXXX` \
             && echo \"%s\" > $SCRIPTFILE \
             && %s $SCRIPTFILE \
             && rm -f $SCRIPTFILE" % (script, self._commands['bsub']['path'])
        ret, out, _ = self.shell.run_sync(cmdline)

        if ret:
            raise rse.NoSuccess("bsub error: %s [%s]" % (out, cmdline))

        # parse the job id. bsub's output looks like this:
        # Job <901545> is submitted to queue <regular>
        lines = out.split("\n")
        lines = [lines for lines in lines if lines != '']  # remove empty

        self._logger.debug('bsub:\n %s' % '\n'.join(lines))

        lsf_job_id = None
        for line in lines:
            if re.search('Job <.+> is submitted to.+queue', line):
                lsf_job_id = re.findall(r'<(.*?)>', line)[0]
                break

        if not lsf_job_id:
            raise Exception("Failed to detect job id after submission.")

        job_id = "[%s]-[%s]" % (self.rm, lsf_job_id)

        self._logger.info("Submitted LSF job with id: %s" % job_id)

        # update job dictionary
        self.jobs[job_obj]['job_id']    = job_id
        self.jobs[job_obj]['submitted'] = job_id

        # set status to 'pending' and trigger callback
        # a guard is in place to not trigger this twice on state updates
        self.jobs[job_obj]['state'] = rsj.PENDING
        job_obj._api()._attributes_i_set('state',
                      self.jobs[job_obj]['state'], job_obj._api()._UP, True)

        # return the job id
        return job_id


    # --------------------------------------------------------------------------
    #
    def _retrieve_job(self, job_id):
        """ see if we can get some info about a job that we don't
            know anything about
        """
        rm, pid = self._adaptor.parse_id(job_id)

        # bjobs -noheader -o 'stat exec_host exit_code submit_time start_time
        # finish_time command job_name delimiter=","' 344077
        # EXIT,summitdev-login1:summitdev-r0c1n12:summitdev-r0c1n12:
        # summitdev-r0c1n12:summitdev-r0c1n12:summitdev-r0c1n12:
        # summitdev-r0c1n12:summitdev-r0c1n12:summitdev-r0c1n12:
        # summitdev-r0c1n12:summitdev-r0c1n12:summitdev-r0c1n12:
        # summitdev-r0c1n12:summitdev-r0c1n12:summitdev-r0c1n12:
        # summitdev-r0c1n12:summitdev-r0c1n12:summitdev-r0c1n12:
        # summitdev-r0c1n12:summitdev-r0c1n12:summitdev-r0c1n12,
        # 2,Oct 16 11:52,Oct 16 11:52,Oct 16 11:53 L,#!/bin/bash;
        # #BSUB -J saga-test;
        # #BSUB -o examplejob.out;
        # #BSUB -e examplejob.err;
        # #BSUB -W 0:10;
        # #BSUB -q batch;
        # #BSUB -P CSC190SPECFEM;
        # #BSUB -nnodes 1;
        # #BSUB -alloc_flags 'gpumps smt4';
        # export  FILENAME=testfile;/bin/touch \$FILENAME " > $SCRIPTFILE &&
        # /sw/sources/lsf-tools/bin/bsub $SCRIPTFILE && rm -f $SCRIPTFILE,
        # saga-test

        ret, out, _ = self.shell.run_sync("%s -noheader -o 'exit_code stat "
                "exec_host submit_time start_time finish_time job_name command "
                "delimiter=\",\"' %s"
                % (self._commands['bjobs']['path'], pid))

        if ret != 0:
            raise rse.NoSuccess("reconnect error '%s': %s" % (job_id, out))

        # the job seems to exist on the backend. let's gather some data
        job_info = dict()

        results = out.split(',')

        if results[0] != '-': job_info['returncode'] = int(results[0])
        else                : job_info['returncode'] = None

        job_info['state']       = _lsf_to_saga_jobstate(results[1])
        job_info['exec_hosts']  = results[2]
        job_info['create_time'] = results[3]
        job_info['start_time']  = results[4]
        job_info['end_time']    = results[5]
        job_info['name']        = results[6]
        job_info['gone']        = False

        cmd  = results[7]
        exe  = cmd.split()[0]
        args = cmd.split()[1:]

        jd = rsj.Description()
        jd.executable = exe
        jd.arguments  = args
        jd.name       = results[7]

        return [job_info, jd]


    # --------------------------------------------------------------------------
    #
    def _job_get_info(self, job_obj):
        """
        get job attributes via bjob
        """

        # if we don't have the job in our dictionary, we don't want it
        if job_obj not in self.jobs:
            raise rse.NoSuccess("Unknown job %s" % job_obj._id)

        # prev. info contains the info collect when _job_get_info
        # was called the last time
        prev_info = self.jobs[job_obj]

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever
        if prev_info['gone'] is True:
            return prev_info

        # curr. info will contain the new job info collect. it starts off
        # as a copy of prev_info (don't use deepcopy because there is an API
        # object in the dict -> recursion)

        curr_info = copy.deepcopy(prev_info)

        rm, pid = self._adaptor.parse_id(job_obj._id)

        # run the LSF 'bjobs' command to get some infos about our job
        # the result of bjobs <id> looks like this:
        #
        # JOBID USER  STAT QUEUE   FROM_HOST   EXEC_HOST  JOB_NAME  SUBMIT_TIME
        # 90154 oweid DONE regular yslogin5-ib ys3833-ib  *FILENAME Nov 11 12:06
        #
        # If we add the -nodeader flag, the first row is ommited

        ret, out, _ = self.shell.run_sync("%s -noheader %s"
                                       % (self._commands['bjobs']['path'], pid))

        if not ret:
            # parse the result
            results = out.split()
            curr_info['state']      = _lsf_to_saga_jobstate(results[2])
            curr_info['exec_hosts'] = results[5]

        else:

            if "Illegal job ID" not in out:
                raise rse.NoSuccess("bjobs error: %s" % out)

            # Let's see if the previous job state was running or pending. in
            # that case, the job is gone now, which can either mean DONE,
            # or FAILED. the only thing we can do is set it to 'DONE'
            curr_info['gone'] = True
            self._logger.warning("job disappeared - set to DONE")

            if prev_info['state'] in [rsj.RUNNING, rsj.PENDING]:
                curr_info['state'] = rsj.DONE
            else:
                curr_info['state'] = rsj.FAILED

        # return the new job info dict
        return curr_info


    # --------------------------------------------------------------------------
    #
    def _job_get_state(self, job_obj):

        return self.jobs[job_obj]['state']


    # --------------------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_obj):

        ret = self.jobs[job_obj]['returncode']

        # FIXME: 'None' should cause an exception
        if ret is None: return None
        else          : return int(ret)


    # --------------------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_obj):

        return self.jobs[job_obj]['exec_hosts']


    # --------------------------------------------------------------------------
    #
    def _job_get_create_time(self, job_obj):

        return self.jobs[job_obj]['create_time']


    # --------------------------------------------------------------------------
    #
    def _job_get_start_time(self, job_obj):

        return self.jobs[job_obj]['start_time']


    # --------------------------------------------------------------------------
    #
    def _job_get_end_time(self, job_obj):
        # FIXME: convert to EPOCH
        return self.jobs[job_obj]['end_time']


    # --------------------------------------------------------------------------
    #
    def _job_cancel(self, job_obj):
        """
        cancel the job via 'qdel'
        """

        rm, pid     = self._adaptor.parse_id(job_obj._id)
        ret, out, _ = self.shell.run_sync("%s %s\n"
                                       % (self._commands['bkill']['path'], pid))

        if ret:
            raise rse.NoSuccess("qdel error: %s" % out)

        # assume the job was succesfully canceled
        self.jobs[job_obj]['state'] = rsj.CANCELED


    # --------------------------------------------------------------------------
    #
    def _job_wait(self, job_obj, timeout):
        """
        wait for the job to finish or fail
        """

        time_start = time.time()
        rm, pid    = self._adaptor.parse_id(job_obj._id)

        while True:

            state = self.jobs[job_obj]['state']  # this gets updated in the bg.

            if state in [rsj.DONE, rsj.FAILED, rsj.CANCELED]:
                return True

            # avoid busy poll
            time.sleep(SYNC_WAIT_UPDATE_INTERVAL)

            # check if we hit timeout
            if timeout >= 0:
                if time.time() - time_start > timeout:
                    return False


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        """
        implements cpi.job.Service.create_job()
        """

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       False
                         }

        # create a new job object
        job_obj = rsj.Job(_adaptor=self._adaptor,
                         _adaptor_state=adaptor_state)

        # add job to internal list of known jobs.
        self.jobs[job_obj._adaptor] = {
            'state':        rsj.NEW,
            'job_id':       None,
            'exec_hosts':   None,
            'returncode':   None,
            'create_time':  None,
            'start_time':   None,
            'end_time':     None,
            'gone':         False,
            'submitted':    False
        }

        return job_obj


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, jobid):
        """ Implements cpi.job.Service.get_job()
        """

        # try to get some information about this job
        job_info, job_desc = self._retrieve_job(jobid)


        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         # TODO: fill job description
                         "job_description": job_desc,
                         "job_schema":      self.rm.schema,
                         "reconnect":       True,
                         "reconnect_jobid": jobid
                         }

        job = rsj.Job(_adaptor=self._adaptor,
                     _adaptor_state=adaptor_state)

        # throw it into our job dictionary.
        self.jobs[job._adaptor] = job_info
        return job


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        """
        implements cpi.job.Service.get_url()
        """

        return self.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """
        implements cpi.job.Service.list()
        """

        ids = list()
        ret, out, _ = self.shell.run_sync("%s -a"
                                         % self._commands['bjobs']['path'])

        if ret != 0 and len(out) > 0:
            # ignore errors with no output (no job found)
            # FIXME: check stderr
            raise rse.NoSuccess("bjobs error: %s" % out)

        if not ret:
            # call succeeded, output looks like this:
            # 112059.svc.uc.futuregrid testjob oweidner 0 Q batch
            # 112061.svc.uc.futuregrid testjob oweidner 0 Q batch
            for line in out.split("\n"):
                if len(line.split()) > 1:
                    elems = line.split()[0].split('.')
                    jobid = "[%s]-[%s]" % (self.rm, elems[0])
                    ids.append(str(jobid))

        return ids


    # --------------------------------------------------------------------------
    #
    def container_run(self, jobs):

        # TODO: this is not optimized yet
        for job in jobs:
            job.run()


    # --------------------------------------------------------------------------
    #
    def container_wait(self, jobs, mode, timeout):

        if timeout:
            raise rse.NoSuccess("bulk wait timeout is not implemented")

        # TODO: this is not optimized yet
        for job in jobs:
            job.wait()


    # --------------------------------------------------------------------------
    #
    def container_cancel(self, jobs, timeout):

        if timeout:
            raise rse.NoSuccess("bulk cancel timeout is not implemented")

        # TODO: this is not optimized yet
        for job in jobs:
            job.cancel()


# ------------------------------------------------------------------------------
#
class LSFJob(cpi.Job):
    """
    implements cpi.job.Job
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        # initialize parent class
        _cpi_base = super(LSFJob, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    def _get_impl(self):
        return self


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements cpi.job.Job.init_instance()
        """
        # init_instance is called for every new Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._name    = self.jd.get(rsj.NAME)
            self._started = True
        else:
            self._id      = None
            self._name    = self.jd.get(rsj.NAME)
            self._started = False

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ implements cpi.job.Job.get_state()
        """
        return self.js._job_get_state(job_obj=self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements cpi.job.Job.wait()
        """
        if not self._started:
            raise rse.IncorrectState("job has not been started")

        self.js._job_wait(job_obj=self, timeout=timeout)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """
        implements cpi.job.Job.cancel()
        """

        if not self._started:
            raise rse.IncorrectState("job has not been started")

        self.js._job_cancel(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """
        implements cpi.job.Job.run()
        """

        self._id = self.js._job_run(self)
        self._started = True


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """
        implements cpi.job.Job.get_service_url()
        """

        return self.js.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        """
        implements cpi.job.Job.get_id()
        """

        return self._id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name(self):
        """
        Implements cpi.job.Job.get_name()
        """

        return self._name


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        """
        implements cpi.job.Job.get_exit_code()
        """

        if not self._started:
            return None

        return self.js._job_get_exit_code(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """
        implements cpi.job.Job.get_created()
        """

        if not self._started:
            return None

        return self.js._job_get_create_time(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """
        implements cpi.job.Job.get_started()
        """

        if not self._started:
            return None

        return self.js._job_get_start_time(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """
        implements cpi.job.Job.get_finished()
        """

        if not self._started:
            return None

        # FIXME: convert to EPOCH
        return self.js._job_get_end_time(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """
        implements cpi.job.Job.get_execution_hosts()
        """

        if not self._started:
            return None

        return self.js._job_get_execution_hosts(self)


# ------------------------------------------------------------------------------

