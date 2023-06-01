
__author__    = "Mark Santcroos, Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2015, The RADICAL Project"
__license__   = "MIT"


""" PBSPro job adaptor implementation
"""


import re
import os
import time
import datetime
import threading

from urllib.parse import parse_qs

import radical.utils as ru

from ...              import exceptions as rse
from ...utils         import pty_shell  as rsups
from ...              import job        as api
from ..               import base       as a_base
from ..cpi            import job        as cpi
from ..cpi            import decorators as cpi_decs


SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL

SYNC_WAIT_UPDATE_INTERVAL = 1  # seconds
MONITOR_UPDATE_INTERVAL   = 5  # seconds


# --------------------------------------------------------------------
#
class _job_state_monitor(threading.Thread):
    """ thread that periodically monitors job states
    """
    def __init__(self, job_service):

        self.logger = job_service._logger
        self.js = job_service
        self._term = threading.Event()

        super(_job_state_monitor, self).__init__()
        self.setDaemon(True)


    def stop(self):
        self._term.set()


    def run(self):

        # we stop the monitoring thread when we see the same error 3 times in
        # a row...
        error_type_count = dict()

        while not self._term.is_set ():

            try:
                # FIXME: do bulk updates here! we don't want to pull information
                # job by job. that would be too inefficient!
                jobs = self.js.jobs

                for job_id in list(jobs.keys()) :

                    job_info = jobs[job_id]

                    # we only need to monitor jobs that are not in a
                    # terminal state, so we can skip the ones that are
                    # either done, failed or canceled
                    if  job_info['state'] not in api.FINAL:

                        # Store the current state since the current state
                        # variable is updated when _job_get_info is called
                        pre_update_state = job_info['state']

                        new_job_info = self.js._job_get_info(job_id, reconnect=False)
                        self.logger.debug("Job monitoring thread updating Job "
                                          "%s (old state: %s, new state: %s)" %
                                          (job_id, pre_update_state, new_job_info['state']))

                        # fire job state callback if 'state' has changed
                        if  new_job_info['state'] != pre_update_state:
                            job_obj = job_info['obj']
                            job_obj._attributes_i_set('state',
                                                      new_job_info['state'],
                                                      job_obj._UP, True)

                        # update job info
                        jobs[job_id] = new_job_info

            except Exception as e:
                import traceback
                traceback.print_exc ()
                self.logger.warning("Exception caught in job monitoring thread: %s" % e)

                # check if we see the same error again and again
                error_type = str(e)
                if  error_type not in error_type_count :
                    error_type_count = dict()
                    error_type_count[error_type]  = 1
                else :
                    error_type_count[error_type] += 1
                    if  error_type_count[error_type] >= 3 :
                        self.logger.error("too many monitoring errors -- stop")
                        return

            finally :
                time.sleep (MONITOR_UPDATE_INTERVAL)


# --------------------------------------------------------------------
#
def _to_saga_jobstate(job_state, retcode, logger=None):
    '''
    translates one-letter batch system state to saga
    '''
    # H: Job is held.
    # Q: Job is queued (eligible to run or routed.)
    # S: Job is suspended.
    # W: Job is waiting for its execution time to be reached.
    # R: Job is running.
    # E: Job is exiting after having run
    # T: Job is being moved to new location.
    # X: Subjob has completed execution or has been deleted (PBSPro)
    # F: Job is Finished (PBSPro)
    # C: Job is completed after having run (Torque)

    if   job_state == 'C':     # Torque
        if retcode ==  0 : ret = api.DONE
        else             : ret = api.FAILED
    elif job_state == 'F':     # PBSPro
        if retcode ==  0 : ret = api.DONE
        else             : ret = api.FAILED
    elif job_state == 'H': ret = api.PENDING
    elif job_state == 'Q': ret = api.PENDING
    elif job_state == 'S': ret = api.PENDING
    elif job_state == 'W': ret = api.PENDING
    elif job_state == 'R': ret = api.RUNNING
    elif job_state == 'E': ret = api.RUNNING
    elif job_state == 'T': ret = api.RUNNING
    elif job_state == 'X': ret = api.CANCELED
    else                 : ret = api.UNKNOWN

    if logger:
        logger.debug('check state: %s', job_state)
        logger.debug('use   state: %s', ret)

    return ret


# --------------------------------------------------------------------
#
def _script_generator(url, logger, jd, ppn, gres, version, is_cray=False,
                      queue=None):
    """ generates a batch script from a SAGA job description
    """
    pbs_params  = ''

    if jd.name:
        pbs_params += '#PBS -N %s\n' % jd.name

    if jd.output:
        stdout_dir = ''
        if jd.working_directory and not os.path.isabs(jd.output):
            # user provided a relative path for STDOUT
            stdout_dir = '%s/' % jd.working_directory
        pbs_params += '#PBS -o %s%s\n' % (stdout_dir, jd.output)

    if jd.error:
        stderr_dir = ''
        if jd.working_directory and not os.path.isabs(jd.error):
            # user provided a relative path for STDERR
            stderr_dir = '%s/' % jd.working_directory
        pbs_params += '#PBS -e %s%s\n' % (stderr_dir, jd.error)

    if jd.wall_time_limit:
        pbs_params += '#PBS -l walltime=%s:%s:00\n' \
            % (int(jd.wall_time_limit / 60), jd.wall_time_limit % 60)

    if jd.project:
        pbs_params += '#PBS -A %s\n' % jd.project
        if 'PBSPro_1' in version:
            # set both parameters: -P(roject) and -A(accounting)
            # depends on system configuration (check with site admin if needed)
            pbs_params += '#PBS -P %s\n' % jd.project

    if jd.job_contact:
        pbs_params += '#PBS -M %s\n' % jd.job_contact
        pbs_params += '#PBS -m abe\n'  # sends email on job abort, begin and end

    # see https://gist.github.com/nobias/5b2373258e595e5242d5
    # The parameter to '-q' can have the following forms:
    #
    #   queue
    #   queue@server
    #   @server
    #
    # where 'server' is the target resource which can be *different* than the
    # submission host.  We interpret 'jd.candidate_hosts[0]' as such a target
    # resource - but only if exactly one `candidate_host` is given.
    #
    # We have to take care to filter out special cases where we abuse
    # `candidate_hosts` for node properties (those are appended to the nodes
    # argument in the resource_list).  This is currently only implemented for
    # "bigflash" on Gordon@SDSC
    #
    # https://github.com/radical-cybertools/radical.saga/issues/406
    #
    # Parse candidate_hosts, node properties are appended to the nodes
    # argument in the resource_list.
    queue_spec = queue or jd.queue or ''

    node_properties = []
    if jd.candidate_hosts:
        if 'BIG_FLASH' in jd.candidate_hosts:
            node_properties.append('bigflash')
        elif len(jd.candidate_hosts) == 1:
            queue_spec += '@%s' % jd.candidate_hosts[0]
        else:
            raise rse.NotImplemented(
                'unsupported candidate_hosts [%s]' % jd.candidate_hosts)

    if queue_spec:
        pbs_params += '#PBS -q %s\n' % queue_spec

    if jd.processes_per_host:
        logger.info('Override detected ppn (%d) with user ppn (%d)' %
                    (ppn, jd.processes_per_host))
        ppn = jd.processes_per_host

    # if total_cpu_count is not defined, we assume 1
    if not jd.total_cpu_count:
        jd.total_cpu_count = 1

    # Request enough nodes to cater for the number of cores requested
    nnodes = jd.total_cpu_count / ppn
    if jd.total_cpu_count % ppn > 0:
        nnodes += 1

    # TODO: Special cases for PBS/TORQUE on Cray
    if any(v in version for v in ['PBSPro_10', '4.2.7']):       # edison, hopper
        pbs_params += '#PBS -l mppwidth=%s\n' % jd.total_cpu_count
    elif any(v in version for v in ['PBSPro_12', 'PBSPro_13']): # archer
        pbs_params += '#PBS -l select=%d\n' % nnodes
    elif is_cray:                                               # kraken, jaguar
        pbs_params += '#PBS -l size=%s\n' % jd.total_cpu_count
    elif node_properties:
        pbs_params += '#PBS -l nodes=%d:ppn=%d%s\n' % (
            nnodes, ppn, ''.join([':%s' % prop for prop in node_properties]))
    else:
        pbs_params += '#PBS -l select=%d:ncpus=%d\n' % (nnodes, ppn)

    # Process Generic Resource specification request
    if gres:
        pbs_params += '#PBS -l gres=%s\n' % gres

    for l_option in jd.system_architecture.get('options', []):
        pbs_params += '#PBS -l %s\n' % l_option

    if is_cray:
        # qsub on Cray systems complains about the -V option:
        # Warning:
        # Your job uses the -V option, which requests that all of your
        # current shell environment settings (9913 bytes) be exported to
        # it.  This is not recommended, as it causes problems for the
        # batch environment in some cases.
        pbs_params += "#PBS -V \n"

    if jd.environment:
        pbs_params += '#PBS -v %s\n' % \
            ','.join(['"%s=%s"' % (k, v) for k, v in jd.environment.items()])

    exec_script = '\nexport SAGA_PPN=%d\n' % ppn
    if jd.working_directory:
        # older PBS installations might not accept this option
        # if jd.working_directory:
        #     pbs_params += '#PBS -d %s \n' % jd.working_directory
        # (*) a workaround is to do an explicit 'cd'
        exec_script += 'export PBS_O_WORKDIR=%s \n' % jd.working_directory
        exec_script += 'mkdir -p %s\n' % jd.working_directory
        exec_script += 'cd       %s\n' % jd.working_directory

    if jd.executable:
        exec_script += '\n%s ' % jd.executable
    if jd.arguments:
        exec_script += ' '.join(jd.arguments)

    # escape all double quotes and dollar signs, otherwise 'echo |' further
    # down won't work only escape '$' in args and exec, not in the params
    exec_script = exec_script.replace('$', '\\$')

    pbscript = '\n#!/bin/bash\n\n%s%s\n' % (pbs_params, exec_script)

    pbscript = pbscript.replace('"', '\\"')
    return pbscript


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.pbsprojob"
_ADAPTOR_SCHEMAS       = ["pbspro", "pbspro+ssh", "pbspro+gsissh"]
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [api.NAME,
                          api.EXECUTABLE,
                          api.ARGUMENTS,
                          api.CANDIDATE_HOSTS,
                          api.ENVIRONMENT,
                          api.INPUT,
                          api.OUTPUT,
                          api.ERROR,
                          api.QUEUE,
                          api.PROJECT,
                          api.WALL_TIME_LIMIT,
                          api.WORKING_DIRECTORY,
                          api.WALL_TIME_LIMIT,
                          api.SPMD_VARIATION,
                          api.PROCESSES_PER_HOST,
                          api.TOTAL_CPU_COUNT,
                          api.TOTAL_GPU_COUNT,
                          api.SYSTEM_ARCHITECTURE,
                          api.JOB_CONTACT],
    "job_attributes":    [api.EXIT_CODE,
                          api.EXECUTION_HOSTS,
                          api.CREATED,
                          api.STARTED,
                          api.FINISHED],
    "metrics":           [api.STATE],
    "callbacks":         [api.STATE],
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
The PBSPro adaptor allows to run and manage jobs on
`PBS <http://www.pbsworks.com/>`_
controlled HPC clusters.
""",
    "example": "examples/jobs/pbsjob.py",
    "schemas": {"pbspro":        "connect to a local cluster",
                "pbspro+ssh":    "connect to a remote cluster via SSH",
                "pbspro+gsissh": "connect to a remote cluster via GSISSH"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {
    "name"        : _ADAPTOR_NAME,
    "version"     : "v0.1",
    "schemas"     : _ADAPTOR_SCHEMAS,
    "capabilities": _ADAPTOR_CAPABILITIES,
    "cpis": [
        {
        "type": "radical.saga.job.Service",
        "class": "PBSProJobService"
        },
        {
        "type": "radical.saga.job.Job",
        "class": "PBSProJob"
        }
    ]
}


###############################################################################
# The adaptor class
class Adaptor (a_base.Base):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by
        the SAGA engine), and which registers the CPI implementation classes
        which provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self):

        a_base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime.datetime(1970,1,1)


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
            raise rse.BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


###############################################################################
#
class PBSProJobService (cpi.Service):
    """ implements cpi.Service
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._mt  = None
        _cpi_base = super(PBSProJobService, self)
        _cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor

    # ----------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # ----------------------------------------------------------------
    #
    def close(self):

        if  self.mt :
            self.mt.stop()
            self.mt.join(10)  # don't block forever on join()

        self._logger.info("Job monitoring thread stopped.")
        self.finalize(True)


    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):

        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm      = rm_url
        self.session = session
        self.ppn     = None
        self.is_cray = False
        self.queue   = None
        self.shell   = None
        self.jobs    = dict()
        self.gres    = None

        # the monitoring thread - one per service instance
        self.mt = _job_state_monitor(job_service=self)
        self.mt.start()

        rm_scheme = rm_url.scheme
        pty_url   = ru.Url(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query:
            for key, val in parse_qs(rm_url.query).items():
                if   key == 'queue'   : self.queue   =     val[0]
                elif key == 'craytype': self.is_cray =     val[0]
                elif key == 'ppn'     : self.ppn     = int(val[0])
                elif key == 'gres'    : self.gres    =     val[0]

        # we need to extract the scheme for PTYShell. That's basically the
        # job.Service Url without the pbs+ part. We use the PTYShell to execute
        # pbs commands either locally or via gsissh or ssh.
        scheme_elems = rm_scheme.split('+')
        if   'gsissh' in scheme_elems : pty_url.scheme = "gsissh"
        elif 'ssh'    in scheme_elems : pty_url.scheme = "ssh"
        else                          : pty_url.scheme = "fork"

        # these are the commands that we need in order to interact with PBS.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note available.
        self._commands = {'pbsnodes': None,
                          'qstat':    None,
                          'qsub':     None,
                          'qdel':     None}

        self.shell = rsups.PTYShell(pty_url, self.session)

      # self.shell.set_initialize_hook(self.initialize)
      # self.shell.set_finalize_hook(self.finalize)

        self.initialize()
        return self.get_api()


    # ----------------------------------------------------------------
    #
    def initialize(self):
        # check if all required pbs tools are available
        for cmd in list(self._commands.keys()):

            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret:
                raise rse.NoSuccess("Error finding PBS tools: %s" % out)

            else:
                path = out.strip()  # strip removes newline
                if cmd in ['qdel', 'qsub']:  # doesn't support --version!
                    self._commands[cmd] = {"path":    path,
                                           "version": "?"}
                else:
                    ret, out, _ = self.shell.run_sync("%s --version" % cmd)
                    if ret:
                        raise rse.NoSuccess("Error finding PBS tools: %s" % out)
                    else:
                        # version is reported as: "version: x.y.z"
                        version = out  # .strip().split()[1]

                        # add path and version to the command dictionary
                        self._commands[cmd] = {"path":    path,
                                               "version": version}

        self._logger.info("Found PBS tools: %s" % self._commands)

        #
        # TODO: Get rid of this, as I dont think there is any
        #       justification that Cray's are special
        #
        # let's try to figure out if we're working on a Cray machine.
        # naively, we assume that if we can find the 'aprun' command in the
        # path that we're logged in to a Cray machine.
        if self.is_cray == "":
            ret, out, _ = self.shell.run_sync('which aprun')
            if ret:
                self.is_cray = ""
            else:
                self._logger.info("Host '%s' seems to be a Cray machine." \
                    % self.rm.host)
                self.is_cray = "unknowncray"
        else:
            self._logger.info("Assuming host is a Cray since 'craytype' is set to: %s" % self.is_cray)

        #
        # Get number of processes per node
        #
        if self.ppn:
            self._logger.debug("Using user specified 'ppn': %d" % self.ppn)
            return

        # TODO: this is quite a hack. however, it *seems* to work quite
        #       well in practice.
        if any(ver in self._commands['qstat']['version'] for ver in ('PBSPro_13', 'PBSPro_12', 'PBSPro_11.3')):
            ret, out, _ = self.shell.run_sync('unset GREP_OPTIONS; %s -a | grep -E "resources_available.ncpus"' % \
                                               self._commands['pbsnodes']['path'])
        else:
            ret, out, _ = self.shell.run_sync('unset GREP_OPTIONS; %s -a | grep -E "(np|pcpu|pcpus)[[:blank:]]*=" ' % \
                                               self._commands['pbsnodes']['path'])

        if ret:
            raise rse.NoSuccess("Error running pbsnodes: %s" % out)
        else:
            # this is black magic. we just assume that the highest occurrence
            # of a specific np is the number of processors (cores) per compute
            # node. this equals max "PPN" for job scripts
            ppn_list = dict()
            for line in out.split('\n'):
                np = line.split(' = ')
                if len(np) == 2:
                    np_str = np[1].strip()
                    if np_str == '<various>':
                        continue
                    else:
                        np = int(np_str)
                    if np in ppn_list:
                        ppn_list[np] += 1
                    else:
                        ppn_list[np] = 1
            self.ppn = max(ppn_list, key=ppn_list.get)
            self._logger.debug("Found the following 'ppn' configurations: %s. "
                "Using %s as default ppn."  % (ppn_list, self.ppn))

    # ----------------------------------------------------------------
    #
    def _job_run(self, job_obj):
        """ runs a job via qsub
        """

        # get the job description
        jd       = job_obj.get_description()
        job_name = jd.name

        # normalize working directory path
        if  jd.working_directory :
            jd.working_directory = os.path.normpath (jd.working_directory)

        if not jd.queue:
            jd.queue = self.queue

        try:
            # create a PBS job script from SAGA job description
            # TODO: make member method
            script = _script_generator(url=self.rm, logger=self._logger,
                                       jd=jd, ppn=self.ppn, gres=self.gres,
                                       version=self._commands['qstat']['version'],
                                       is_cray=self.is_cray, queue=jd.queue
                                      )

            self._logger.info("Generated PBS script: %s" % script)
        except Exception as e:
            raise rse.BadParameter('error generating PBS script') from e

        # try to create the working directory (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #          compute nodes.
        if jd.working_directory:
            self._logger.info("Create working directory %s" % jd.working_directory)
            ret, out, _ = self.shell.run_sync("mkdir -p %s" % jd.working_directory)
            if ret:
                # something went wrong
                raise rse.NoSuccess("Couldn't create workdir - %s" % out)

        # Now we want to execute the script. This process consists of two steps:
        # (1) we create a temporary file with 'mktemp' and write the contents of
        #     the generated PBS script into it
        # (2) we call 'qsub <tmpfile>' to submit the script to the
        #     queueing system
        cmdline = """
        SCRIPTFILE=`mktemp -t rs.jobscript.XXXXXX` \\
            &&  echo "%s" > $SCRIPTFILE \\
            &&  %s $SCRIPTFILE \\
            &&  rm -f $SCRIPTFILE
            """ %  (script, self._commands['qsub']['path'])
        ret, out, _ = self.shell.run_sync(cmdline)

        if ret:
            # something went wrong
            raise rse.NoSuccess("Error running 'qsub': %s: %s" % (out, cmdline))

        # parse the job id. qsub usually returns just the job id, but
        # sometimes there are a couple of lines of warnings before.
        # if that's the case, we log those as 'warnings'
        lines = out.split('\n')
        lines = [_f for _f in lines if _f]  # remove empty lines

        if not lines:
            self._logger.error('qsub: no output (%s)' % cmdline)
            raise RuntimeError('no output from qsub')

        if len(lines) > 1:
            self._logger.warning('qsub: %s' % ''.join(lines[:-2]))

        # we asssume job id is in the last line
        bs_id  = lines[-1].strip().split('.')[0]
        job_id = "[%s]-[%s]" % (self.rm, bs_id)
        state  = api.PENDING
        self._logger.info("Submitted PBS job with id: %s" % job_id)

        # populate job info dict
        self.jobs[job_id] = {'obj'         : job_obj,
                             'job_id'      : job_id,
                             'name'        : job_name,
                             'state'       : state,
                             'exec_hosts'  : None,
                             'returncode'  : None,
                             'create_time' : time.time(),
                             'start_time'  : None,
                             'end_time'    : None,
                             'gone'        : False
                             }

        self._logger.info ("assign job id  %s / %s / %s to watch list (%s)"
                        % (job_name, job_id, job_obj, list(self.jobs.keys())))

        # set status to 'pending' and manually trigger callback
        job_obj._attributes_i_set('state', state, job_obj._UP, True)

        return job_id


    # ----------------------------------------------------------------
    #
    def _job_get_info(self, job_id, reconnect):
        """ Get job information attributes via qstat.
        """

        # If we don't have the job in our dictionary, we don't want it,
        # unless we are trying to reconnect.
        if not reconnect and job_id not in self.jobs:
            raise rse.NoSuccess("Unknown job id: %s" % job_id)

        if not reconnect:
            # job_info contains the info collect when _job_get_info
            # was called the last time
            job_info = self.jobs[job_id]

            # if the 'gone' flag is set, there's no need to query the job
            # state again. it's gone forever
            if job_info['gone'] is True:
                return job_info
        else:
            # Create a template data structure
            job_info = {
                'job_id':       job_id,
                'state':        api.UNKNOWN,
                'name':         None,
                'exec_hosts':   None,
                'returncode':   None,
                'create_time':  None,
                'start_time':   None,
                'end_time':     None,
                'gone':         False
            }

        rm, pid = self._adaptor.parse_id(job_id)

        # run the PBS 'qstat' command to get job's info
        # options: "-f" - long format; "-x" - includes finished jobs
        pbs_version = self._commands['qstat']['version']
        if 'PBSPro_1' in pbs_version: qstat_flags = '-f'
        else                        : qstat_flags = '-fx'

        ret, out, _ = self.shell.run_sync("unset GREP_OPTIONS; %s %s %s | "
                "grep -E -i '(job_state)|(Job_Name)|(exec_host)|(exit_status)|"
                 "(ctime)|(start_time)|(stime)|(mtime)'"
                % (self._commands['qstat']['path'], qstat_flags, pid))

        if ret:

            if reconnect:
                raise rse.NoSuccess("Couldn't reconnect job %s: %s" % (job_id, out))

            if ("Unknown Job Id" in out):
                # Let's see if the last known job state was running or pending. in
                # that case, the job is gone now, which can either mean DONE,
                # or FAILED. the only thing we can do is set it to 'DONE'
                job_info['gone'] = True
                # TODO: we can also set the end time?
                self._logger.warning("Previously running job has disappeared. "
                        "This probably means that the backend doesn't store "
                        "information about finished jobs. Setting state to 'DONE'.")

                if  job_info['state'] in [api.RUNNING, api.PENDING]:
                    job_info['state']      = api.DONE
                    job_info['returncode'] = 0  # we are guessing here...
                else:
                    job_info['state'] = api.UNKNOWN

                if not job_info['start_time']:
                    # inaccurate guess, but better than nothing
                    job_info['start_time'] = time.time()

                if not job_info['end_time']:
                    # inaccurate guess, but better than nothing
                    job_info['end_time'] = time.time()

            else:
                # something went wrong
                raise rse.NoSuccess("Error running qstat: %s" % out)
        else:

            # The job seems to exist on the backend. let's process some data.

            # TODO: make the parsing "contextual", in the sense that it takes
            #       the state into account.

            # parse the egrep result. this should look something like this:
            #     job_state = C
            #     exec_host = i72/0
            #     exit_status = 0
            results = out.split('\n')
            for line in results:

                if '=' in line:
                    k, v = line.split('=', 1)
                    k    = k.strip()
                    v    = v.strip()

                    if   k in ['job_state'  ]: job_state               = v
                    elif k in ['job_name'   ]: job_info['name']        = v
                    elif k in ['exit_status',  # TORQUE / PBS Pro
                               'Exit_status']: job_info['returncode' ] = int(v)
                    elif k in ['exec_host'  ]: job_info['exec_hosts' ] = v
                    elif k in ['start_time',   # TORQUE / PBS Pro
                               'stime'      ]: job_info['start_time' ] = v
                    elif k in ['ctime'      ]: job_info['create_time'] = v
                    elif k in ['mtime'      ]: job_info['end_time'   ] = v

                  # FIXME: qstat will not tell us time zones, so we cannot
                  #        convert to EPOCH (which is UTC).  We thus take times
                  #        ourself.  A proper solution would be to either do the
                  #        time conversion on the target host, or to inspect
                  #        time zone settings on the host.
                  #
                  # NOTE:  PBS Pro doesn't provide "end time", but
                  #        "resources_used.walltime" could be added up to the
                  #        start time.  Alternatively, we can use mtime, (latest
                  #        modification time) which is generally also end time.
                  #        TORQUE has an "comp_time" (completion? time), that is
                  #        generally the same as mtime.
                  #
                  #        For now we use mtime for both TORQUE and PBS Pro.

            # split exec hosts list if set
            if job_info.get('exec_host'):
                job_info['exec_hosts'] = job_info['exec_hosts'].split('+')

            retcode = job_info.get('returncode', -1)
            job_info['state'] = _to_saga_jobstate(job_state, retcode)

            # FIXME: workaround for time zone problem described above
            if job_info['state'] in [api.RUNNING] + api.FINAL \
                and not job_info['start_time']:
                job_info['start_time'] = time.time()

            if job_info['state'] in api.FINAL \
                and not job_info['end_time']:
                job_info['end_time'] = time.time()

        # return the updated job info
        return job_info


    # ----------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        """ get the job's state
        """
        return self.jobs[job_id]['state']

    # ----------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_id):
        """ get the job's exit code
        """
        ret = self.jobs[job_id]['returncode']

        if ret is None : return None
        else           : return int(ret)

    # ----------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_id):
        """ get the job's exit code
        """
        return self.jobs[job_id]['exec_hosts']

    # ----------------------------------------------------------------
    #
    def _job_get_create_time(self, job_id):
        """ get the job's creation time
        """
        return self.jobs[job_id]['create_time']

    # ----------------------------------------------------------------
    #
    def _job_get_start_time(self, job_id):
        """ get the job's start time
        """
        return self.jobs[job_id]['start_time']

    # ----------------------------------------------------------------
    #
    def _job_get_end_time(self, job_id):
        """ get the job's end time
        """
        return self.jobs[job_id]['end_time']

    # ----------------------------------------------------------------
    #
    def _job_cancel(self, job_id):
        """ cancel the job via 'qdel'
        """
        if self.jobs[job_id]['state'] in api.FINAL:
            # job is already final - nothing to do
            return

        rm, pid = self._adaptor.parse_id(job_id)

        ret, out, _ = self.shell.run_sync("%s %s\n"
                    % (self._commands['qdel']['path'], pid))

        if ret:
            raise rse.NoSuccess("Error canceling job via 'qdel': %s" % out)

        # assume the job was successfully canceled
        self.jobs[job_id]['state'] = api.CANCELED

        if not self.jobs[job_id]['end_time']:
            self.jobs[job_id]['end_time'] = time.time()


    # ----------------------------------------------------------------
    #
    def _job_wait(self, job_id, timeout):
        """ wait for the job to finish or fail
        """
        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(job_id)

        while True:
            state = self.jobs[job_id]['state']  # this gets updated in the bg.

            if state in api.FINAL:
                return True

            # avoid busy poll
            time.sleep(SYNC_WAIT_UPDATE_INTERVAL)

            # check if we hit timeout
            if timeout >= 0:
                time_now = time.time()
                if time_now - time_start > timeout:
                    return False

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        """ implements cpi.Service.get_url()
        """
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       False
                         }

        # create and return a new job object
        return api.Job(_adaptor=self._adaptor,
                       _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, job_id):
        """ Implements cpi.Service.get_job()

            Re-create job instance from a job-id.
        """

        # If we already have the job info, we just pass the current info.
        if job_id in self.jobs :
            return self.jobs[job_id]['obj']

        # Try to get some initial information about this job (again)
        job_info = self._job_get_info(job_id, reconnect=True)

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        jd = api.Description()
        for k,v in job_info.get("description",{}).items():
            jd[k] = v
        jd.name = job_info.get('name')
        adaptor_state = {"job_service":     self,
                         # TODO: fill job description
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       True,
                         "reconnect_jobid": job_id
                         }

        job_obj = api.Job(_adaptor=self._adaptor,
                          _adaptor_state=adaptor_state)

        # throw it into our job dictionary.
        job_info['obj']   = job_obj
        self.jobs[job_id] = job_info

        return job_obj

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        """ implements cpi.Service.get_url()
        """
        return self.rm

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """ implements cpi.Service.list()
        """
        ids = []

        ret, out, _ = self.shell.run_sync("unset GREP_OPTIONS; %s | grep `whoami`" %
                                          self._commands['qstat']['path'])

        if ret and out:
            raise rse.NoSuccess("failed to list jobs via 'qstat': %s" % out)

        elif ret and not out:
            # qstat | grep `` exits with 1 if the list is empty
            pass
        else:
            for line in out.split("\n"):
                # output looks like this:
                # 112059.svc.uc.futuregrid testjob oweidner 0 Q batch
                # 112061.svc.uc.futuregrid testjob oweidner 0 Q batch
                if len(line.split()) > 1:
                    job_id = "[%s]-[%s]" % (self.rm, line.split()[0].split('.')[0])
                    ids.append(str(job_id))

        return ids


    # ----------------------------------------------------------------
    #
    def container_run (self, jobs) :

        self._logger.debug ("container run: %s"  %  str(jobs))

        # TODO: this is not optimized yet
        for job in jobs:
            job.run ()


    # ----------------------------------------------------------------
    #
    def container_wait (self, jobs, mode, timeout) :

        self._logger.debug ("container wait: %s"  %  str(jobs))

        # TODO: this is not optimized yet
        for job in jobs:
            job.wait ()


    # ----------------------------------------------------------------
    #
    def container_cancel (self, jobs, timeout) :

        self._logger.debug ("container cancel: %s"  %  str(jobs))

        # TODO: this is not optimized yet
        for job in jobs:
            job.cancel (timeout)


# ------------------------------------------------------------------------------
#
class PBSProJob (cpi.Job):
    """ implements cpi.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        _cpi_base = super(PBSProJob, self)
        _cpi_base.__init__(api, adaptor)

    def _get_impl(self):
        return self

    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements cpi.Job.init_instance()
        """
        # init_instance is called for every new Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._name    = self.jd.get(api.NAME)
            self._started = True
        else:
            self._id      = None
            self._name    = self.jd.get(api.NAME)
            self._started = False

        return self.get_api()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ implements cpi.Job.get_state()
        """
        if  self._started is False:
            return api.NEW

        return self.js._job_get_state(job_id=self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements cpi.Job.wait()
        """
        if self._started is False:
            raise rse.IncorrectState("Can't wait for job that hasn't been started")
        else:
            self.js._job_wait(job_id=self._id, timeout=timeout)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements cpi.Job.cancel()
        """
        if self._started is False:
            raise rse.IncorrectState("Can't wait for job that hasn't been started")
        else:
            self.js._job_cancel(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements cpi.Job.run()
        """
        self._id = self.js._job_run(self._api())
        self._started = True

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """ implements cpi.Job.get_service_url()
        """
        return self.js.rm

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        """ implements cpi.Job.get_id()
        """
        return self._id

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name (self):
        """ Implements cpi.Job.get_name() """
        return self._name

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        """ implements cpi.Job.get_exit_code()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_exit_code(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """ implements cpi.Job.get_created()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_create_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """ implements cpi.Job.get_started()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_start_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """ implements cpi.Job.get_finished()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_end_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """ implements cpi.Job.get_execution_hosts()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_execution_hosts(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description(self):
        """ implements cpi.Job.get_execution_hosts()
        """
        return self.jd


# ------------------------------------------------------------------------------

