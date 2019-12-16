
__author__    = 'The RADICAL Team'
__copyright__ = 'Copyright 2012-2019, The SAGA Project'
__license__   = 'MIT'


'''
LSF job adaptor implementation
'''

import re
import os
import time
import threading
import datetime
import tempfile

import radical.utils as ru
from urllib.parse import parse_qs

from ...job           import constants   as c
from ...utils         import pty_shell   as rsups
from ...              import job         as api
from ...              import exceptions  as rse
from ..               import base        as a_base
from ..cpi            import job         as cpi
from ..cpi            import decorators  as cpi_decs

SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL

# newer (2019) Intel LSF hosts have SMT default to 4
# FIXME: move to a resource config
SMT = 4


# ------------------------------------------------------------------------------
#
class _job_state_monitor(threading.Thread):
    '''
    thread that periodically monitors job states
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, job_service):

        self.logger = job_service._logger
        self.js     = job_service
        self._stop  = threading.Event()

        super(_job_state_monitor, self).__init__()
        self.setDaemon(True)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._stop.set()


    # --------------------------------------------------------------------------
    #
    def run(self):

        while not self._stop.isSet():

            try:
                # do bulk updates here! we don't want to pull information
                # job by job. that would be too inefficient!
                for job in self.js._jobs:
                    job_info = self.js._jobs[job]

                    # if the job hasn't been started, we can't update its
                    # state. we can tell if a job has been started if it
                    # has a job id
                    if job_info.get('id'):
                        # we only need to monitor jobs that are not in a
                        # terminal state, so we can skip the ones that are
                        # either done, failed or canceled
                        state = job_info['state']
                        if state not in [api.DONE, api.FAILED, api.CANCELED]:

                            new_info = self.js._job_get_info(job)

                            if new_info['state'] != state:
                                # fire job state callback if 'state' has changed
                                self.logger.info('update Job %s (state: %s)'
                                                % (job, new_info['state']))
                                job._api()._attributes_i_set('state',
                                        new_info['state'], job._api()._UP, True)

                            # replace job info
                            self.js._jobs[job] = new_info

                time.sleep(MONITOR_UPDATE_INTERVAL)

            except Exception:
                self.logger.exception('job monitoring thread failed')


# ------------------------------------------------------------------------------
#
def _lsf_to_saga_jobstate(lsfjs):
    '''
    translates a lsf one-letter state to saga
    '''
    if   lsfjs in ['RUN']                     : return api.RUNNING
    elif lsfjs in ['WAIT', 'PEND']            : return api.PENDING
    elif lsfjs in ['DONE']                    : return api.DONE
    elif lsfjs in ['UNKNOWN', 'ZOMBI', 'EXIT']: return api.FAILED
    elif lsfjs in ['USUSP', 'SSUSP', 'PSUSP'] : return api.SUSPENDED
    else                                      : return api.UNKNOWN


# ------------------------------------------------------------------------------
#
def _lsfscript_generator(url, logger, jd, ppn, lsf_version):
    '''
    generates an LSF script from a SAGA job description
    '''
    lsf_params  = str()
    exec_n_args = str()

    lsf_bsubs   = ''
    command     = ''
    env_string  = ''

    if jd.executable: command += '%s ' % (jd.executable)
    if jd.arguments : command += ' '.join(jd.arguments)

    if       jd.queue and     queue: lsf_bsubs += '#BSUB -q %s \n' % queue
    elif     jd.queue and not queue: lsf_bsubs += '#BSUB -q %s \n' % jd.queue
    elif not jd.queue and     queue: lsf_bsubs += '#BSUB -q %s \n' % queue

    if jd.name             : lsf_bsubs += '#BSUB -J %s \n' % jd.name
    if jd.job_contact      : lsf_bsubs += '#BSUB -u %s \n' % jd.job_contact
    if jd.working_directory: lsf_bsubs += '#BSUB -cwd %s \n' \
                                                     %  jd.working_directory
    if jd.wall_time_limit  : lsf_bsubs += '#BSUB -W %s:%s \n' \
                                          % (int(jd.wall_time_limit / 60),
                                             int(jd.wall_time_limit % 60))

    # if working directory is set, we want stdout to end up in the
    # working directory as well, unless it containes a specific
    # path name - otherwise we pass `output` as is.
    if jd.output:
        if os.path.isabs(jd.output): path = ''
        elif jd.working_directory  : path = '%s/' % jd.working_directory
        else                       : path = ''
        lsf_bsubs += '#BSUB -o %s%s \n' % (path, jd.output)

    # same holds for stderr
    if jd.error:
        if os.path.isabs(jd.error): path = ''
        elif jd.working_directory : path = '%s/' % jd.working_directory
        else                      : path = ''
        lsf_bsubs += '#BSUB -e %s%s \n' % (path, jd.error)


    env_string += 'export RADICAL_SAGA_SMT=%d' % SMT
    if jd.environment:
        for k,v in jd.environment.items():
            env_string += ' %s=%s' % (k,v)


    env_variable_list = 'export RADICAL_SAGA_SMT=%d' % SMT
    if jd.environment:
        for key in jd.environment:
            env_variable_list += ' %s=%s ' % (key, jd.environment[key])

    # a workaround is to do an explicit 'cd'
    if jd.working_directory is not None:
        lsf_params += '#BSUB -cwd %s \n' % jd.working_directory

    if jd.output is not None:
        # if working directory is set, we want stdout to end up in
        # the working directory as well, unless it containes a specific
        # path name.
        if jd.working_directory is not None:
            if os.path.isabs(jd.output):
                lsf_params += '#BSUB -o %s \n' % jd.output
            else:
                # user provided a relative path for STDOUT. in this case
                # we prepend the working directory path before passing
                # it on to LSF.
                lsf_params += '#BSUB -o %s/%s \n' \
                            % (jd.working_directory, jd.output)
        else:
            lsf_params += '#BSUB -o %s \n' % jd.output

    if jd.error is not None:
        # if working directory is set, we want stderr to end up in
        # the working directory as well, unless it contains a specific
        # path name.
        if jd.working_directory is not None:
            if os.path.isabs(jd.error):
                lsf_params += '#BSUB -e %s \n' % jd.error
            else:
                # user provided a relative path for STDERR. in this case
                # we prepend the working directory path before passing
                # it on to LSF.
                lsf_params += '#BSUB -e %s/%s \n' \
                            % (jd.working_directory, jd.error)
        else:
            lsf_params += '#BSUB -e %s \n' % jd.error

    elif jd.project:
        lsf_bsubs += '#BSUB -P %s \n' % jd.project


    if jd.queue:
        lsf_params += '#BSUB -q %s \n' % jd.queue

    if not jd.total_gpu_count: total_gpu_count = 1
    else                     : total_gpu_count = jd.total_gpu_count

    hostname = url.host

    if not hostname or 'localhost' in hostname:
        out, _, ret = ru.sh_callout('hostname -f')
        if ret: hostname = os.environ.get('HOSTNAME', '')
        else  : hostname = out.strip()

    if not hostname:
        raise RuntimeError('cannot determine target host f or %s' % url)

    if   'summitdev' in hostname: cpn = 20 * SMT
    elif 'summit'    in hostname: cpn = 42 * SMT
    else: raise ValueError('LSF host (%s) not yet supported' % hostname)

    if   'summitdev' in hostname: gpn = 4
    elif 'summit'    in hostname: gpn = 6

    cpu_nodes = int(total_cpu_count / cpn)
    if total_cpu_count > (cpu_nodes * cpn):
        cpu_nodes += 1

    # escape all double quotes and dollar signs, otherwise 'echo |'
    # further down won't work
    # only escape '$' in args and exe. not in the params
    exec_n_args = exec_n_args.replace('$', '\\$')

    lsf_bsubs += '#BSUB -nnodes %s \n' % str(nodes)
    lsf_bsubs += '#BSUB -alloc_flags "gpumps smt%d" \n' % SMT

    # escape double quotes and dollar signs, otherwise 'echo |'
    # further down won't work
    # only escape '$' in args and exe. not in the bsubs
    command   = command.replace('$', '\\$')
    lsfscript = '\n#!/bin/bash \n%s\n%s\n%s' % (lsf_bsubs, env_string, command)
    lsfscript = lsfscript.replace('"', '\\"')

    return lsfscript


# ------------------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0


# ------------------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = 'radical.saga.adaptors.lsfjob'
_ADAPTOR_SCHEMAS       = ['lsf', 'lsf+ssh', 'lsf+gsissh']
_ADAPTOR_OPTIONS       = []

# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
# TODO: FILL ALL IN FOR SLURM
_ADAPTOR_CAPABILITIES  = {
    'jdes_attributes'  : [c.NAME,
                          c.EXECUTABLE,
                          c.PRE_EXEC,
                          c.POST_EXEC,
                          c.ARGUMENTS,
                          c.ENVIRONMENT,
                          c.SPMD_VARIATION,
                          c.TOTAL_CPU_COUNT,
                        # c.TOTAL_GPU_COUNT,
                          c.NUMBER_OF_PROCESSES,
                          c.PROCESSES_PER_HOST,
                          c.THREADS_PER_PROCESS,
                          c.WORKING_DIRECTORY,
                        # c.INTERACTIVE,
                          c.INPUT,
                          c.OUTPUT,
                          c.ERROR,
                          c.FILE_TRANSFER,
                          c.CLEANUP,
                          c.WALL_TIME_LIMIT,
                          c.TOTAL_PHYSICAL_MEMORY,
                          c.CPU_ARCHITECTURE,
                        # c.OPERATING_SYSTEM_TYPE,
                          c.CANDIDATE_HOSTS,
                          c.QUEUE,
                          c.PROJECT,
                          c.JOB_CONTACT],
    'job_attributes'   : [c.EXIT_CODE,
                          c.EXECUTION_HOSTS,
                          c.CREATED,
                          c.STARTED,
                          c.FINISHED],
    'metrics'          : [c.STATE,
                          c.STATE_DETAIL],
    'contexts'         : {'ssh'      : 'public/private keypair',
                          'x509'     : 'X509 proxy for gsissh',
                          'userpass' : 'username/password pair for simple ssh'}
}

# ------------------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS,
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : '''
        The LSF adaptor allows to run and manage jobs on
        `LSF <https://en.wikipedia.org/wiki/Platform_LSF>`_
        controlled HPC clusters.
        ''',
    'example': 'examples/jobs/lsfjob.py',
    'schemas': {'lsf':        'connect to a local cluster',
                'lsf+ssh':    'conenct to a remote cluster via SSH',
                'lsf+gsissh': 'connect to a remote cluster via GSISSH'}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.2',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'cpis'             : [
        {
            'type'     : 'radical.saga.job.Service',
            'class'    : 'LSFJobService'
        },
        {
            'type'     : 'radical.saga.job.Job',
            'class'    : 'LSFJob'
        }
    ]
}


# ------------------------------------------------------------------------------
# The adaptor class
#
class Adaptor (a_base.Base):
    '''
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self):

        a_base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime.datetime(1970,1,1)


    # --------------------------------------------------------------------------
    #
    def sanity_check(self):
        pass

    # --------------------------------------------------------------------------
    #
    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if not match or len(match.groups()) != 2:
            raise rse.BadParameter('Cannot parse job id [%s]' % id)

        return (match.group(1), match.group(2))


# ------------------------------------------------------------------------------
#
class LSFJobService(cpi.Service):
    '''
    implements cpi.job.Service
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(LSFJobService, self)
        _cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor
        self._shell   = None
        self._monitor = None

    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if  self._monitor:
            self._monitor.stop()
            self._monitor.join(10)  # don't block forever on join()

        self._logger.info('Job monitoring thread stopped.')

        if  self._shell:
            self._shell.finalize(True)
            del(self._shell)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        '''
        service instance constructor
        '''

        self.rm      = rm_url
        self.session = session
        self.ppn     = 1
        self._shell  = None
        self._jobs   = dict()

        # the monitoring thread - one per service instance
        self._monitor = _job_state_monitor(job_service=self)
        self._monitor.start()

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
        rm_scheme = rm_url.scheme
        pty_url   = ru.Url(rm_url)
        elems     = rm_scheme.split('+')
        if   'gsissh' in elems: pty_url.scheme = 'gsissh'
        elif 'ssh'    in elems: pty_url.scheme = 'ssh'
        else                  : pty_url.scheme = 'fork'

        # these are the commands that we need in order to interact with LSF.
        # the adaptor will try to find them when it first opens the shell
        # connection, and bails out in case they are not available.
        self._commands = {'bqueues': None,
                          'bjobs':   None,
                          'bsub':    None,
                          'bkill':   None}

        self._shell = rsups.PTYShell(pty_url, self.session)

        # check if all required lsf tools are available
        for cmd in self._commands:
            ret, out, _ = self._shell.run_sync('which %s ' % cmd)
            if ret:
                raise rse.NoSuccess('Could not find LSF tools: %s' % out)

            self._commands[cmd] = out.strip()

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
>>>>>>> devel

        _, out, _ = self._shell.run_sync('bsub -V 2>&1 | cut -f 1')
        self._version = out.split()[1].strip()
        self._logger.info('LSF version: %s' % self._version)


      # # see if we can get some information about the cluster, e.g.,
      # # different queues, number of processes per node, etc.
      # # TODO: this is quite a hack. however, it *seems* to work quite
      # #       well in practice.
      # ret, out, _ = self._shell.run_sync(
      #                   'unset GREP_OPTIONS; %s -a | grep -E "(np|pcpu)"'
      #                   % self._commands['pbsnodes']['path'])
      # if ret != 0:
      #
      #     raise rse.NoSuccess('Error running pbsnodes: %s' % out)
      # else:
      #    # this is black magic. we just assume that the highest occurence
      #    # of a specific np is the number of processors(cores) per compute
      #    # node. this equals max 'PPN' for job scripts
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
    def _job_run(self, jd):
        '''
        runs a job via qsub
        '''
        # get the job description
        job_name = jd.name

        # normalize working directory path
        if  jd.working_directory:
            jd.working_directory = os.path.normpath(jd.working_directory)

        try:
            # create an LSF job script from SAGA job description
            script = _lsfscript_generator(url=self.rm, logger=self._logger,
                                jd=jd, ppn=self.ppn,
                                lsf_version=self._commands['bjobs']['version'])

            self._logger.info('Generated LSF script: %s' % script)

        except Exception as e:
            raise rse.BadParameter(str(e))

        # try to create the working directory (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #          compute nodes.
        if jd.working_directory:
            pwd = jd.working_directory
            self._logger.info('Creating working directory %s' % pwd)
            ret, out, _ = self._shell.run_sync('mkdir -p %s' % pwd)

            if ret:
                raise rse.NoSuccess("Couldn't create workdir %s" % out)

        # (1) create a temporary file with 'mktemp' and write the contents of
        #     the generated PBS script into it
        # (2) call 'qsub <tmpfile>' to submit the script to the batch system
        #
        cmdline = \
            'SCRIPTFILE=`mktemp -p $HOME -t SAGA-Python-LSFJobScript.XXXXXX` ' \
            '&& echo "%s" > $SCRIPTFILE ' \
            '&& %s $SCRIPTFILE ' \
            '&& rm -f $SCRIPTFILE' % (script, self._commands['bsub']['path'])
        ret, out, _ = self._shell.run_sync(cmdline)

        if ret:
            raise rse.NoSuccess('bsub error: %s [%s]' % (out, cmdline))

        # parse the job id. bsub's output looks like this:
        # Job <901545> is submitted to queue <regular>
        lines = out.split('\n')
        lines = out.split("\n")
        lines = [line.strip() for line in lines if line.strip()]

        self._logger.debug('bsub:\n %s' % '\n'.join(lines))

        lsf_job_id = None
        for line in lines:
            if re.search('Job <.+> is submitted to.+queue', line):
                lsf_job_id = re.findall(r'<(.*?)>', line)[0]
                break

        if not lsf_job_id:
            raise Exception('Failed to detect job id after submission.')

        job_id = '[%s]-[%s]' % (self.rm, lsf_job_id)
        self._logger.info('Submitted LSF job with id: %s' % job_id)

        # populate job info dict
        state = api.PENDING
        self._jobs[job_id] = {'job_id'      : job_id,
                              'name'        : job_name,
                              'state'       : state,
                              'exec_hosts'  : None,
                              'returncode'  : None,
                              'create_time' : time.time(),
                              'start_time'  : None,
                              'end_time'    : None,
                              'gone'        : False
                             }

        # return the job id
        return job_id


    # --------------------------------------------------------------------------
    #
    def _retrieve_job(self, job_id):
        '''
        see if we can get some info about a job that we don't
            know anything about
        '''
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

        ret, out, _ = self._shell.run_sync("%s -noheader -o 'exit_code stat "
                "exec_host submit_time start_time finish_time job_name command "
                "delimiter=\",\"' %s"
                % (self._commands['bjobs']['path'], pid))

        if ret != 0:
            raise rse.NoSuccess('reconnect error "%s": %s' % (job_id, out))

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

        jd = api.Description()
        jd.executable = exe
        jd.arguments  = args
        jd.name       = results[7]

        return [job_info, jd]


    # --------------------------------------------------------------------------
    #
    def _job_get_info(self, job_id):
        '''
        get job attributes via bjob
        '''

        # if we don't have the job in our dictionary, we don't want it
        if job_id not in self._jobs:
            raise rse.NoSuccess('Unknown job %s' % job_id)

        # prev. info contains the info collect when _job_get_info
        # was called the last time
        prev_info = self._jobs[job_id]

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever
        if prev_info['gone'] is True:
            return prev_info

        # curr. info will contain the new job info collect. it starts off
        # as a copy of prev_info (don't use deepcopy because there is an API
        # object in the dict -> recursion)

        curr_info = copy.deepcopy(prev_info)

        rm, pid = self._adaptor.parse_id(job_id)

        # run the LSF 'bjobs' command to get some infos about our job
        # the result of bjobs <id> looks like this:
        #
        # JOBID USER  STAT QUEUE   FROM_HOST   EXEC_HOST  JOB_NAME  SUBMIT_TIME
        # 90154 oweid DONE regular yslogin5-ib ys3833-ib  *FILENAME Nov 11 12:06
        #
        # If we add the -nodeader flag, the first row is ommited

        ret, out, _ = self._shell.run_sync('%s -noheader %s'
                                       % (self._commands['bjobs']['path'], pid))

        if not ret:
            # parse the result
            results = out.split()
            curr_info['state']      = _lsf_to_saga_jobstate(results[2])
            curr_info['exec_hosts'] = results[5]

        else:

            if 'Illegal job ID' not in out:
                raise rse.NoSuccess('bjobs error: %s' % out)

            # Let's see if the previous job state was running or pending. in
            # that case, the job is gone now, which can either mean DONE,
            # or FAILED. the only thing we can do is set it to 'DONE'
            curr_info['gone'] = True
            self._logger.warning('job disappeared - set to DONE')

            if prev_info['state'] in [api.RUNNING, api.PENDING]:
                curr_info['state'] = api.DONE
            else:
                curr_info['state'] = api.FAILED

        # return the new job info dict
        return curr_info


    # --------------------------------------------------------------------------
    #
    def _job_get_state(self, job_id):

        return self._jobs[job_id]['state']


    # --------------------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_id):

        ret = self._jobs[job_id]['returncode']

        # FIXME: 'None' should cause an exception
        if ret is None: return None
        else          : return int(ret)


    # --------------------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_id):

        return self._jobs[job_id]['exec_hosts']


    # --------------------------------------------------------------------------
    #
    def _job_get_create_time(self, job_id):

        return self._jobs[job_id]['create_time']


    # --------------------------------------------------------------------------
    #
    def _job_get_start_time(self, job_id):

        return self._jobs[job_id]['start_time']


    # --------------------------------------------------------------------------
    #
    def _job_get_end_time(self, job_id):
        # FIXME: convert to EPOCH
        return self._jobs[job_id]['end_time']


    # --------------------------------------------------------------------------
    #
    def _job_cancel(self, job_id):
        '''
        cancel the job via 'qdel'
        '''

        rm, pid     = self._adaptor.parse_id(job_id)
        ret, out, _ = self._shell.run_sync('%s %s\n'
                                       % (self._commands['bkill']['path'], pid))

        if ret:
            raise rse.NoSuccess('qdel error: %s' % out)

        # assume the job was succesfully canceled
        self._jobs[job_id]['state'] = api.CANCELED


    # --------------------------------------------------------------------------
    #
    def _job_wait(self, job_id, timeout):
        '''
        wait for the job to finish or fail
        '''

        time_start = time.time()
        rm, pid    = self._adaptor.parse_id(job_id)

        while True:

            state = self._jobs[job_id]['state']  # this gets updated in the bg.

            if state in [api.DONE, api.FAILED, api.CANCELED]:
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
        '''
        implements cpi.job.Service.create_job()
        '''

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {'job_service':     self,
                         'job_description': jd,
                         'job_schema':      self.rm.schema,
                         'reconnect':       False
                         }

        # create a new job object
        job_obj = api.Job(_adaptor=self._adaptor,
                         _adaptor_state=adaptor_state)

        return job_obj


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, jobid):
        '''
        Implements cpi.job.Service.get_job()
        '''

        # try to get some information about this job
        job_info, job_desc = self._retrieve_job(jobid)


        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {'job_service':     self,
                         # TODO: fill job description
                         'job_description': job_desc,
                         'job_schema':      self.rm.schema,
                         'reconnect':       True,
                         'reconnect_jobid': jobid
                         }

        job = api.Job(_adaptor=self._adaptor,
                     _adaptor_state=adaptor_state)

        # throw it into our job dictionary.
        self._jobs[job._adaptor] = job_info
        return job


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        '''
        implements cpi.job.Service.get_url()
        '''

        return self.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        '''
        implements cpi.job.Service.list()
        '''

        ids = list()
        ret, out, _ = self._shell.run_sync('%s -a'
                                         % self._commands['bjobs']['path'])

        if ret != 0 and len(out) > 0:
            # ignore errors with no output (no job found)
            # FIXME: check stderr
            raise rse.NoSuccess('bjobs error: %s' % out)

        if not ret:
            # call succeeded, output looks like this:
            # 112059.svc.uc.futuregrid testjob oweidner 0 Q batch
            # 112061.svc.uc.futuregrid testjob oweidner 0 Q batch
            for line in out.split('\n'):
                if len(line.split()) > 1:
                    elems = line.split()[0].split('.')
                    jobid = '[%s]-[%s]' % (self.rm, elems[0])
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
            raise rse.NoSuccess('bulk wait timeout is not implemented')

        # TODO: this is not optimized yet
        for job in jobs:
            job.wait()


    # --------------------------------------------------------------------------
    #
    def container_cancel(self, jobs, timeout):

        if timeout:
            raise rse.NoSuccess('bulk cancel timeout is not implemented')

        # TODO: this is not optimized yet
        for job in jobs:
            job.cancel()


# ------------------------------------------------------------------------------
#
class LSFJob(cpi.job.Job):
    '''
    implements cpi.job.Job
    '''

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
        '''
        implements cpi.job.Job.init_instance()
        '''
        # init_instance is called for every new Job object
        # that is created
        self.jd = job_info['job_description']
        self.js = job_info['job_service']

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._name    = self.jd.get(api.NAME)
            self._started = True
        else:
            self._id      = None
            self._name    = self.jd.get(api.NAME)
            self._started = False

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        '''
        implements cpi.job.Job.wait()
        '''
        if not self._started:
            raise rse.IncorrectState('job has not been started')

        self.js._job_wait(job_id=self._id, timeout=timeout)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        '''
        implements cpi.job.Job.cancel()
        '''

        if not self._started:
            raise rse.IncorrectState('job has not been started')

        self.js._job_cancel(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        '''
        implements cpi.job.Job.run()
        '''

        self._id = self.js._job_run(self.jd)

        # trigger state callback
        self._attributes_i_set('state', api.PENDING, self._UP, True)

        self._started = True


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        '''
        implements cpi.job.Job.get_state()
        '''
        self._state = self.js._job_get_state(job_id=self._id)
        return self._state


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        '''
        implements cpi.job.Job.get_service_url()
        '''

        return self.js.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        '''
        implements cpi.job.Job.get_id()
        '''

        return self._id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name(self):
        '''
        Implements cpi.job.Job.get_name()
        '''

        return self._name


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        '''
        implements cpi.job.Job.get_exit_code()
        '''

        if not self._started:
            return None

        return self.js._job_get_exit_code(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        '''
        implements cpi.job.Job.get_created()
        '''

        if not self._started:
            return None

        return self.js._job_get_create_time(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        '''
        implements cpi.job.Job.get_started()
        '''

        if not self._started:
            return None

        return self.js._job_get_start_time(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        '''
        implements cpi.job.Job.get_finished()
        '''

        if not self._started:
            return None

        # FIXME: convert to EPOCH
        return self.js._job_get_end_time(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        '''
        implements cpi.job.Job.get_execution_hosts()
        '''

        if not self._started:
            return None

        return self.js._job_get_execution_hosts(self)


# ------------------------------------------------------------------------------

