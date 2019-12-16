# -*- coding: utf-8 -*-

__author__    = "Andre Merzky, Christian P.-Llamas, Ole Weidner, " \
                "Thomas Schatz, Alexander Grill"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" SGE job adaptor implementation
"""

import re
import os
import time

from cgi      import parse_qs
from io import StringIO
from datetime import datetime

import radical.utils as ru

from ...              import exceptions as rse
from ...              import utils      as rsu
from ...utils         import pty_shell  as rsups
from ...utils         import misc       as rsumisc
from ...              import job        as api
from ...adaptors      import base       as a_base
from ...adaptors.cpi  import job        as cpi
from ...adaptors.cpi  import decorators as cpi_decs
from ...job           import constants  as c


SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL


_QSTAT_JOB_STATE_RE = re.compile(
    r"^([^ ]+) ([0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}) (.+)$")


class SgeKeyValueParser(object):
    """
    Parser for SGE commands returning lines with key-value pairs.
    It takes into account multi-line key-value pairs.
    It works as an iterator returning (key, value) tuples or as a dictionary.
    It allows to filter for keys.
    """

    KEY_VALUE_RE = re.compile(r"^([^ ]+) +(.+)$")

    def __init__(self, stream, filter_keys=None, key_suffix=None):
        """
        :param stream: an string or a file-like object implementing readline()
        :param filter_keys: an iterable with the list of keys of interest.
        :param key_suffix: a key suffix to remove when parsing
        """

        # check whether it is an string or a file-like object
        if isinstance(stream, str):
            self.stream = StringIO(stream)
        else:
            self.stream = stream

        self.filter_keys = set(filter_keys) if filter_keys is not None else None
        self.key_suffix = key_suffix

    def __next__(self):
        """
        Return the next key-value pair.
        :return: (key, value)
        """

        key, value = None, None
        while key is None:
            line = self.stream.readline()
            if len(line) == 0:
                raise StopIteration

            line = line.rstrip(" \n")

            # check for multi-line options
            while len(line) > 0 and line[-1] == "\\":
                line = line[:-1] + self.stream.readline() \
                                              .rstrip(" \n") \
                                              .lstrip(" ")

            m = self.KEY_VALUE_RE.match(line)
            if m is not None:

                key, value = m.groups()

                if  self.key_suffix is not None and \
                    key.endswith(self.key_suffix):
                    key = key[:-len(self.key_suffix)]

                if  self.filter_keys is not None and \
                    key not in self.filter_keys:
                    key = None  # skip this pair

        return key, value


    def __iter__(self):
        return self


    def as_dict(self):
        """
        Parses the key-value pairs and return them as a dictionary.
        :return: a dictionary containing key-value pairs parsed from
                 a SGE command.
        """

        d = dict()
        for key, value in self:
            d[key] = value

        return d


# --------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    logger.error(message)
    raise exception(message)


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.sgejob"
_ADAPTOR_SCHEMAS       = ["sge", "sge+ssh", "sge+gsissh"]

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [c.NAME,
                          c.EXECUTABLE,
                          c.ARGUMENTS,
                          c.ENVIRONMENT,
                          c.INPUT,
                          c.OUTPUT,
                          c.ERROR,
                          c.QUEUE,
                          c.PROJECT,
                          c.WALL_TIME_LIMIT,
                          c.WORKING_DIRECTORY,
                          c.SPMD_VARIATION,
                          c.TOTAL_CPU_COUNT,
                          c.PROCESSES_PER_HOST,
                          c.CANDIDATE_HOSTS,
                          c.TOTAL_PHYSICAL_MEMORY],
    "job_attributes":    [c.EXIT_CODE,
                          c.EXECUTION_HOSTS,
                          c.CREATED,
                          c.STARTED,
                          c.FINISHED],
    "metrics":           [c.STATE],
    "contexts":          {"ssh": "SSH public/private keypair",
                          "x509": "GSISSH X509 proxy context",
                          "userpass": "username/password pair (ssh)"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":  """
The SGE (Sun/Oracle Grid Engine) adaptor allows to run and manage jobs on
`SGE <http://en.wikipedia.org/wiki/Oracle_Grid_Engine>`_ controlled HPC
clusters.
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
    "name"         : _ADAPTOR_NAME,
    "version"      : "v0.1",
    "schemas"      : _ADAPTOR_SCHEMAS,
    "capabilities" : _ADAPTOR_CAPABILITIES,
    "cpis"         : [
                         {
                             "type": "radical.saga.job.Service",
                             "class": "SGEJobService"
                         },
                         {
                             "type": "radical.saga.job.Job",
                             "class": "SGEJob"
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

        a_base.Base.__init__(self, _ADAPTOR_INFO)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime(1970,1,1)

        self.purge_on_start   = self._cfg['purge_on_start']
        self.purge_older_than = self._cfg['purge_older_than']
        self.base_workdir     = self._cfg['base_workdir']


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
class SGEJobService (cpi.Service):
    """ implements cpi.Service
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
        self.accounting = False
        self.temp_path = self._adaptor.base_workdir


        rm_scheme = rm_url.scheme
        pty_url   = ru.Url (rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query is not None:
            for key, val in parse_qs(rm_url.query).items():
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
        # out in case they are not available.
        self._commands = {'qstat': None,
                          'qsub':  None,
                          'qdel':  None,
                          'qconf': None,
                          'qacct': None}

        self.shell = rsups.PTYShell(pty_url, self.session)

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
        for cmd in list(self._commands.keys()):
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding SGE tools: %s" % out
                log_error_and_raise(message, rse.NoSuccess, self._logger)
            else:
                path = out.strip()  # strip removes newline

                ret, out, _ = self.shell.run_sync("%s -help" % cmd)
                if ret != 0:
                    # fix for a bug in certain qstat versions that return
                    # '1' after a successfull qstat -help:
                    # https://github.com/radical-cybertools/radical.saga/issues/163
                    if cmd == 'qstat':
                        version = out.strip().split('\n')[0]
                    else:
                        message = "Error finding SGE tools: %s" % out
                        log_error_and_raise(message, rse.NoSuccess,
                                            self._logger)
                else:
                    # version is reported in the first row of the
                    # help screen, e.g., GE 6.2u5_1
                    version = out.strip().split('\n')[0]

                    # add path and version to the command dictionary
                    self._commands[cmd] = {"path"   : "unset GREP_OPTIONS; %s"
                                                      % path,
                                           "version": version}

        self._logger.info("Found SGE tools: %s" % self._commands)

        # determine the available processing elements
        ret, out, _ = self.shell.run_sync('%s -spl' %
                      (self._commands['qconf']['path']))
        if ret != 0:
            message = "Error running 'qconf': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)
        else:
            for pe in out.split('\n'):
                if pe != '':
                    self.pe_list.append(pe)
            self._logger.debug("Available processing elements: %s" %
                (self.pe_list))

        # find out mandatory and optional memory attributes
        ret, out, _ = self.shell.run_sync('%s -sc'
                                         % (self._commands['qconf']['path']))
        if ret != 0:
            message = "Error running 'qconf': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)
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
            self._logger.debug("Optional  memory attr: %s" % optional_attrs)
            self._logger.debug("Mandatory memory attr: %s" % mandatory_attrs)

        # find out user specified memory attributes in job.Service URL
        if self.memreqs is None:
            flags = []
        else:
            flags, _ = self.__parse_memreqs(self.memreqs)

        # if there are mandatory memory attributes store them and check that
        # they were specified in the job.Service URL

        if not (mandatory_attrs == []):
            self.mandatory_memreqs = mandatory_attrs
            missing_flags = []
            for attr in mandatory_attrs:
                if attr not in flags:
                    missing_flags.append(attr)
            if missing_flags:
                message = "The following memory attribute(s) are mandatory " \
                          "in your SGE environment and thus must be " \
                          "specifiedin the job service URL: %s" \
                          % ' '.join(missing_flags)
                log_error_and_raise(message, rse.BadParameter, self._logger)

        # if memory attributes were specified in the job.Service URL, check that
        # they correspond to existing optional or mandatory memory attributes

        invalid_attrs = []
        for f in flags:
            if not (f in optional_attrs or f in mandatory_attrs):
                invalid_attrs.append(f)

        if invalid_attrs:
            message = "The following memory attribute(s) were specified in " \
                      "the job.Service URL but are not valid memory " \
                      "attributes in your SGE environment: %s" \
                      % ' '.join(invalid_attrs)
            log_error_and_raise(message, rse.BadParameter, self._logger)

        # check if accounting is activated
        qres = self.__kvcmd_results('qconf', '-sconf',
                                    filter_keys=["reporting_params"])
        self.accounting = "reporting_params" in qres and \
                          "accounting=true"  in qres["reporting_params"]

        # purge temporary files
        if self._adaptor.purge_on_start:
            cmd = "find " + self.temp_path + \
                  " -type f -mtime +%d -print -delete | wc -l" \
                  % self._adaptor.purge_older_than
            ret, out, _ = self.shell.run_sync(cmd)
            if ret == 0 and out != "0":
                self._logger.info("Purged %s temporary files" % out)


    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)


    # ----------------------------------------------------------------
    #
    # private members
    #

    def __sge_to_saga_jobstate(self, sge_state):
        """
        Translates an SGE one-letter state to SAGA
        """

        try:
            if sge_state.startswith("d"):

                # when a qdel is done the state is prefixed with a d while the
                # termination signal is queued
                sge_state = sge_state[1:]

            return {
                'c'   : c.DONE,
                'E'   : c.RUNNING,
                'H'   : c.PENDING,
                'qw'  : c.PENDING,
                'r'   : c.RUNNING,
                't'   : c.RUNNING,
                'w'   : c.PENDING,
                's'   : c.PENDING,
                'X'   : c.CANCELED,
                'Eqw' : c.FAILED
            }[sge_state]
        except:
            return c.UNKNOWN

    def __parse_memreqs(self, s):
        """
        Simple parser for getting memory requirements flags and multipliers
        from the memreqs part of the job.Service url
        """
        flags = []
        multipliers = []
        while len(s) != 0:
            # find multiplier
            m = re.match(r'\d+\.?\d*|\d*\.?\d+', s)
            if m:
                multipliers.append(float(s[m.start():m.end()]))
                s = s[m.end():]
            else:
                multipliers.append(1.0)
            # find flag
            pos = s.find('~')
            if pos < 0:
                flags.append(s)
                s = ''
            else:
                flags.append(s[:pos])
                s = s[pos + 1:]
        return flags, multipliers


    def __kvcmd_results(self, cmd, cmd_args, *args, **kwargs):
        """
        Runs a SGE command that returns key-value pairs as result and parses
        the results.

        :param cmd: command alias
        :param cmd_args: command arguments
        :param args: parser arguments
        :param kwargs: parser keyword arguments
        :returns: a dictionary if succeeded or None otherwise
        """

        ret, out, _ = self.shell.run_sync(
                              '%s %s' % (self._commands[cmd]['path'], cmd_args))
        if ret == 0:
            return SgeKeyValueParser(out, *args, **kwargs).as_dict()
        return None

    def __remote_mkdir(self, path):
        """
        Creates a directory on the remote host.
        :param path: the remote directory to be created.
        """
        # check if the path exists
        ret, out, _ = self.shell.run_sync(
                       "(test -d %s && echo -n 0) || (mkdir -p %s && echo -n 1)"
                       % (path, path))

        if ret == 0 and out == "1":
            self._logger.info("Remote directory created: %s" % path)

        elif ret != 0:
            # something went wrong
            message = "Couldn't create remote directory - %s" % (out)
            log_error_and_raise(message, rse.NoSuccess, self._logger)

    def __job_info_from_accounting(self, sge_job_id, max_retries=10):
        """
        Returns job information from the SGE accounting using qacct.

        It may happen that when the job exits from the queue system the results
        in the accounting database take some time to appear. To avoid premature
        failing several tries can be done (up to a maximum) with delays of
        1 second in between.

        :param sge_job_id: SGE job id
        :param max_retries: The maximum number of retries in case qacct fails
        :return: job information dictionary
        """

        job_info = None
        retries  = max_retries

        while retries > 0:
            retries -= 1

            qres = self.__kvcmd_results('qacct', "-j %s | grep -E '%s'"
                                       % (sge_job_id,
           "jobname|hostname|qsub_time|start_time|end_time|exit_status|failed"))

            if not qres and retries > 0:
                # sometimes there is a lapse between the job exits from the
                # queue and its information enters in the accounting database
                # let's run qacct again after a delay
                time.sleep(1)
                continue


            # ok, extract job info from qres
            # jobname      test
            # hostname     sge
            # qsub_time    Mon Jun 24 17:24:43 2013  # FIXME: convert to EPOCH
            # start_time   Mon Jun 24 17:24:50 2013  # FIXME: convert to EPOCH
            # end_time     Mon Jun 24 17:44:50 2013  # FIXME: convert to EPOCH
            # failed       0
            # exit_status  0

            if qres.get("failed") == "0": state = c.DONE
            else                        : state = c.FAILED
            job_info = {'state'       : state,
                        'name'        : qres.get("jobname"),
                        'exec_hosts'  : qres.get("hostname"),
                        'create_time' : qres.get("qsub_time"),
                        'start_time'  : qres.get("start_time"),
                        'end_time'    : qres.get("end_time"),
                        'returncode'  : int(qres.get("exit_status", -1)),
                        'gone'        : False
                       }
            break

        return job_info


    def __remote_job_info_path(self, sge_job_id="$JOB_ID"):
        """
        Returns the path of the remote job info file.
        param sge_job_id: the SGE job id, if omitted an enviroment variable
                           representing the job id will be used.
        return: path to the remote job info file
        """

        return "%s/%s" % (self.temp_path, sge_job_id)


    def __clean_remote_job_info(self, sge_job_id):
        """
        Removes the temporary remote file containing job info.
        :param sge_job_id: the SGE job id
        """

        path = self.__remote_job_info_path(sge_job_id)
        ret, out, _ = self.shell.run_sync("rm %s" % path)
        if ret != 0:
            self._logger.debug("Remote job info couldn't be removed: %s" % path)


    def __get_remote_job_info(self, sge_job_id):
        """
        Obtains job info from temporary remote file created by the qsub script.
        :param sge_job_id: the SGE job id
        :return: a dictionary with the job info
        """

        ret, out, _ = self.shell.run_sync("cat %s" % self.__remote_job_info_path(sge_job_id))
        if ret != 0:
            return None

        qres = SgeKeyValueParser(out, key_suffix=":").as_dict()

        if   "signal"      in qres: state = c.CANCELED
        elif "exit_status" in qres: state = c.DONE
        else                      : state = c.RUNNING

        job_info = {'state'       : state,
                    'name'        : qres.get("jobname"),
                    'exec_hosts'  : qres.get("hostname"),
                    'create_time' : qres.get("qsub_time"),
                    'start_time'  : qres.get("start_time"),
                    'end_time'    : qres.get("end_time"),
                    'returncode'  : int(qres.get("exit_status", -1)),
                    'gone'        : False}

        return job_info

    def __generate_qsub_script(self, jd):
        """
        Generates an SGE script from a SAGA job description
        :param jd: job descriptor
        :return: the qsub script
        """

        # SGE parameters

        sge_params = ["#$ -S /bin/bash"]

        if jd.name is not None:
            sge_params += ["#$ -N %s" % jd.name]

        sge_params += ["#$ -V"]

        if jd.environment is not None and len(jd.environment) > 0:
            env_list = ",".join(["%s=%s" % (key, value) for key, value
                                                     in list(jd.environment.items())])
            sge_params += ["#$ -v %s" % env_list]

        if jd.working_directory is not None:
            sge_params += ["#$ -wd %s" % jd.working_directory]

        if jd.output is not None:
            sge_params += ["#$ -o %s" % jd.output]

        if jd.error is not None:
            sge_params += ["#$ -e %s" % jd.error]

        if jd.wall_time_limit is not None:
            hours = int(jd.wall_time_limit / 60)
            minutes = jd.wall_time_limit % 60
            sge_params += ["#$ -l h_rt=%s:%s:00" % (str(hours), str(minutes))]

        queue = self.queue or jd.queue
        if queue is not None:
            sge_params += ["#$ -q %s" % queue]

        if jd.project is not None:
            sge_params += ["#$ -A %s" % str(jd.project)]

        if jd.job_contact is not None:
            sge_params += ["#$ -m be", "#$ -M %s" % jd.contact]

        # memory requirements - TOTAL_PHYSICAL_MEMORY
        # it is assumed that the value passed through jd is always in Megabyte
        if jd.total_physical_memory is not None:
            # this is (of course) not the same for all SGE installations. some
            # use virtual_free, some use a combination of mem_req / h_vmem.
            # It is very annoying. We need some sort of configuration variable
            # that can control this. Yes, ugly and not very saga-ish, but
            # the only way to do this, IMHO...
            if self.memreqs is None:
                raise Exception("""
When using 'total_physical_memory' with the SGE adaptor, the query parameters of
the job.Service URL must define the attributes used by your particular instance
of SGE to control memory allocation.  'virtual_free', 'h_vmem' or 'mem_req' are
commonly encountered examples of such attributes.
A valid job.Service URL could be for instance:

    'sge+ssh://myserver.edu?memreqs=virtual_free~1.5h_vmem'

here the attribute 'virtual_free' would be set to 'total_physical_memory' and
the attribute 'h_vmem' would be set to 1.5*'total_physical_memory' '~' is used
as a separator.
""")

            flags, multipliers = self.__parse_memreqs(self.memreqs)
            for flag, mult in zip(flags, multipliers):
                sge_params += ["#$ -l %s=%sm" % (flag,
                            int(round(mult * int(jd.total_physical_memory))))]

        # check spmd variation. this translates to the SGE qsub -pe flag.
        if jd.spmd_variation is not None:
            if jd.spmd_variation not in self.pe_list:
                raise Exception("'%s' is not valid for jd.spmd_variation. "
                      "Valid are: %s" % (jd.spmd_variation, self.pe_list))

            # if no cores are requested at all, we default to 1

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
            sge_params += ["#$ -pe %s %s" % (jd.spmd_variation,
                                             jd.total_cpu_count or 1)]

        elif jd.total_cpu_count is not None and jd.total_cpu_count > 1:
                raise Exception("jd.total_cpu_count requires that "
                                "jd.spmd_variation is not empty. "
                                "Valid are: %s" % (self.pe_list))


        # CANDEIDATE_HOSTS - this translates to 'qsub -l h="host1|host2|..."'
        if jd.candidate_hosts:
            sge_params.append('#$ -l h="%s"' % '|'.join(jd.candidate_hosts))


        # convert sge params into an string
        sge_params = "\n".join(sge_params)

        # Job info, executable and arguments

        job_info_path = self.__remote_job_info_path()

        script_body = [
            'function aborted() {',
            '  echo Aborted with signal $1.',
            '  echo "signal: $1" >>%s' % job_info_path,
            '  echo "end_time: $(LC_ALL=en_US.utf8 date \'+%%s\')" >>%s'
                    % job_info_path,
            '  exit -1',
            '}',
            'mkdir -p %s' % self.temp_path,
            'for sig in SIGHUP SIGINT SIGQUIT SIGTERM SIGUSR1 SIGUSR2;'
            '    do trap "aborted $sig" $sig; done',
            'echo "hostname: $HOSTNAME" >%s' % job_info_path,
            'echo "jobname: %s" >>%s' % (jd.name, job_info_path),
            'echo "qsub_time: %s" >>%s' % (time.time(), job_info_path),
            'echo "start_time: $(LC_ALL=en_US.utf8 date \'+%%s\')" >>%s'
                   % job_info_path
        ]

        exec_n_args = None
        if jd.executable is not None:
            exec_n_args = jd.executable
            if jd.arguments is not None:
                exec_n_args += " %s" % " ".join(jd.arguments)

        elif jd.arguments is not None:
            raise Exception("jd.arguments defined without jd.executable")

        if exec_n_args is not None:
            script_body += [exec_n_args]

        script_body += [
            'echo "exit_status: $?" >>%s' % job_info_path,
            'echo "end_time: $(LC_ALL=en_US.utf8 date \'+%%s\')" >>%s'
                  % job_info_path
        ]

        # convert exec and args into an string and
        # escape all double quotes and dollar signs, otherwise 'echo |'
        # further down won't work.
        # only escape '$' in args and exe. not in the params
        script_body = "\n".join(script_body).replace('$', '\\$')

        sgescript = "\n#!/bin/bash \n%s \n%s" % (sge_params, script_body)

        return sgescript.replace('"', '\\"')

    # ----------------------------------------------------------------
    #
    # Adaptor internal methods
    #

    def _job_run(self, jd):
        """
        Runs a job via qsub
        """

        if self.queue and jd.queue and self.queue != jd.queue:
            self._logger.warning("Job service has explicit 'queue=%s', "
                                 "but job description uses queue: '%s'. "
                                 "Using '%s'."
                                 % (self.queue, jd.queue, self.queue))

        # In SGE environments with mandatory memory attributes,
        # 'total_physical_memory' must be specified
        if self.mandatory_memreqs and jd.total_physical_memory is None:
            log_error_and_raise("Your SGE environments has mandatory memory "
                                "attributes, so 'total_physical_memory' "
                                "must be specified in your job descriptor",
                                rse.BadParameter, self._logger)

        try:
            # create a SGE job script from SAGA job description
            script = self.__generate_qsub_script(jd)
            self._logger.info("Generated SGE script: %s" % script)

        except Exception as ex:
            log_error_and_raise(str(ex), rse.BadParameter, self._logger)

        # try to create the working/output/error directories (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #           compute nodes.
        if jd.working_directory is not None and len(jd.working_directory) > 0:
            self.__remote_mkdir(jd.working_directory)

        if jd.output is not None and len(jd.output) > 0:
            self.__remote_mkdir(os.path.dirname(jd.output))

        if jd.error is not None and len(jd.error) > 0:
            self.__remote_mkdir(os.path.dirname(jd.output))

        # submit the SGE script
        # Now we want to execute the script. This process consists of two steps:
        # (1) we create a temporary file with 'mktemp' and write the contents of
        #     the generated PBS script into it
        # (2) we call 'qsub <tmpfile>' to submit the script to the queueing
        #     system
        cmdline = 'SCRIPTFILE=`mktemp -t RS-SGEJobScript.XXXXXX` ' \
                  ' && echo "%s" > $SCRIPTFILE ' \
                  ' && %s -notify $SCRIPTFILE ' \
                  ' && rm -f $SCRIPTFILE' \
                  %  (script, self._commands['qsub']['path'])

        ret, out, _ = self.shell.run_sync(cmdline)

        if ret != 0:
            # something went wrong
            message = "Error running qsub: %s [%s]" % (out, cmdline)
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        # stdout contains the job id:
        # Your job 1036608 ("testjob") has been submitted
        sge_job_id = None
        for line in out.split('\n'):
            if line.find("Your job") != -1:
                sge_job_id = line.split()[2]
        if sge_job_id is None:
            message = "Couldn't parse job id from 'qsub' output: %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        job_id = "[%s]-[%s]" % (self.rm, sge_job_id)
        self._logger.info("Submitted SGE job with id: %s" % job_id)

        # add job to internal list of known jobs.
        self.jobs[job_id] = {
            'state':        c.PENDING,
            'name':         jd.name,
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
        """ retrieve job information
        :param job_id: SAGA job id
        :return: job information dictionary
        """

        rm, pid = self._adaptor.parse_id(job_id)

        # check the state of the job
        ret, out, _ = self.shell.run_sync(
                    "%s | tail -n+3 | awk '($1==%s) {{print $5,$6,$7,$8}}'" % (
                    self._commands['qstat']['path'], pid))

        out = out.strip()

        job_info = None

        if ret == 0 and len(out) > 0:

            # job is still in the queue
            # output is something like
            # r 06/24/2013 17:24:50

            m = _QSTAT_JOB_STATE_RE.match(out)
            if not m:
                # something wrong with the result of qstat
                message = "Unexpected qstat results:\n%s" % out.rstrip()
                log_error_and_raise(message, rse.NoSuccess, self._logger)

            state, start_time, queue = m.groups()

            # Convert start time into EPOCH
            start_time = None
            if state in ["r", "t", "s", "S", "T", "d", "E", "Eqw"]:
                try:
                    dt = datetime.strptime(start_time, "%m/%d/%Y %H:%M:%S")
                    start_time = (dt - self._adaptor.epoch).total_seconds()
                except:
                    # keep 'None'
                    pass

            exec_host = None
            if "@" in queue:
                queue, exec_host = queue.split("@")
                exec_host = exec_host.rstrip()

            # if it is an Eqw job it is better to retrieve the information
            # from qacct
            if self.accounting and state == "Eqw":
                job_info = self.__job_info_from_accounting(pid)
                # TODO remove the job from the queue ?

            if job_info is None: # use qstat -j pid
                qres = self.__kvcmd_results(
                'qstat', "-j %s | grep -E 'job_name|submission_time|sge_o_host'"
                % pid, key_suffix=":")

                if qres is not None:

                    # when qstat fails it will fall back to qacct
                    # output is something like
                    # submission_time:            Mon Jun 24 17:24:43 2013
                    # sge_o_host:                 sge

                    state = self.__sge_to_saga_jobstate(state)
                    host  = exec_host or qres.get("sge_o_host")
                    job_info = {'state'       : state,
                                'name'        : qres.get("job_name"),
                                'exec_hosts'  : host,
                                'returncode'  : None,
                                'create_time' : qres.get("submission_time"),
                                'start_time'  : start_time,
                                'end_time'    : None,
                                'gone'        : False
                                }

        # if job already finished or there was an error with qstat
        # try to read the remote job info
        if job_info is None:
            job_info = self.__get_remote_job_info(pid)

        # none of the previous methods gave us job info
        # if accounting is activated use qacct
        if self.accounting and job_info is None:
            job_info = self.__job_info_from_accounting(pid)

        if job_info is None: # Oooops, we couldn't retrieve information from SGE
            message = "Couldn't reconnect to job '%s'" % job_id
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        elems = ["name", "state", "returncode", "exec_hosts", "create_time",
                 "start_time", "end_time", "gone"]
        self._logger.debug("job_info(%s)=[%s]"
                % (pid, ", ".join(["%s=%s" % (k, str(job_info[k]))
                                           for k in elems])))
        return job_info


    # ----------------------------------------------------------------
    #
    def _job_get_info(self, job_id):
        """ get job attributes
        """

        # if we don't have the job in our dictionary, we don't want it
        if job_id not in self.jobs:
            message = "Unknown job ID: %s. Can't update state." % job_id
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        # prev. info contains the info collect when _job_get_info
        # was called the last time
        prev_info = self.jobs[job_id]

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever
        if prev_info['gone'] is True:
            self._logger.warning("Job information is not available anymore.")
            return prev_info

        # if the job is in a terminal state don't expect it to change anymore
        if prev_info["state"] in c.FINAL:
            return prev_info

        # retrieve updated job information
        curr_info = self._retrieve_job(job_id)
        if curr_info is None:
            prev_info["gone"] = True
            return prev_info

        # update the job info cache and return it
        self.jobs[job_id] = curr_info
        return curr_info


    # ----------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        """ get the job's state
        """
        # check if we have already reach a terminal state
        if self.jobs[job_id]['state'] in c.FINAL:
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

        # FIXME: convert to EOPCH
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
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        self.__clean_remote_job_info(pid)

        # assume the job was succesfully canceld
        self.jobs[job_id]['state'] = c.CANCELED


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

            if state == c.UNKNOWN :
                log_error_and_raise("cannot get job state",
                                    rse.IncorrectState, self._logger)

            if state in c.FINAL:
                self.__clean_remote_job_info(pid)
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
        """ implements cpi.Service.get_url()
        """
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       False
                        }

        return api.Job(_adaptor=self._adaptor,
                       _adaptor_state=adaptor_state)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, jobid):
        """ Implements cpi.Service.get_job()
        """

        # try to get some information about this job
        job_info = self._retrieve_job(jobid)

        # save it into our job dictionary.
        self.jobs[jobid] = job_info

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         # TODO: fill job description
                         "job_description": api.Description(),
                         "job_schema":      self.rm.schema,
                         "reconnect":       True,
                         "reconnect_jobid": jobid
                        }

        return api.Job(_adaptor=self._adaptor,
                       _adaptor_state=adaptor_state)


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

        ret, out, _ = self.shell.run_sync(
                           "unset GREP_OPTIONS; %s | grep `whoami`"
                           % self._commands['qstat']['path'])

        if ret != 0 and len(out) > 0:
            message = "Failed to list jobs via 'qstat': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        elif ret != 0 and len(out) == 0:
            # qstat | grep `whoami` exits with 1 if the list is empty
            pass

        else:
            for line in out.split("\n"):
                if len(line.split()) > 1:
                    jobid = "[%s]-[%s]" % (self.rm, line.split()[0])
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
  # def container_cancel (self, jobs, timeout) :
  #     self._logger.debug ("container cancel: %s"  %  str(jobs))
  #     raise rse.NoSuccess ("Not Implemented");


###############################################################################
#
class SGEJob (cpi.Job):
    """ implements cpi.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        _cpi_base = super(SGEJob, self)
        _cpi_base.__init__(api, adaptor)

    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements cpi.Job.init_instance()
        """
        # init_instance is called for every new api.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._name    = self.jd.name
            self._started = True
        else:
            self._id      = None
            self._name    = self.jd.name
            self._started = False

        return self.get_api()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description (self):
        return self.jd

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ implements cpi.Job.get_state()
        """
        if self._started is False:
            # jobs that are not started are always in 'NEW' state
            return api.NEW
        else:
            return self.js._job_get_state(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements cpi.Job.wait()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                rse.IncorrectState, self._logger)
        else:
            self.js._job_wait(self._id, timeout)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements cpi.Job.cancel()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                rse.IncorrectState, self._logger)
        else:
            self.js._job_cancel(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements cpi.Job.run()
        """
        self._id = self.js._job_run(self.jd)
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


# ------------------------------------------------------------------------------

