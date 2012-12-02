# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides the base class and factory methods for the various command-line 
    wrapper, like GSISSH and SSH. 
'''

from cmdlinewrapper_subprocess import SubprocessCommandLineWrapper
from cmdlinewrapper_gsissh import GSISSHCommandLineWrapper
from cmdlinewrapper_ssh import SSHCommandLineWrapper

class CLWException(Exception):
    ''' Raised for CommandLineWrapper exceptions.
    '''

class CommandWrapperResult(object):
    ''' A 3-tuple returned by CommandLineWrapper.run().
    '''
    def __init__(self, command, stdout=None, returncode=None, duration=-1):

        self._command = command
        self._stdout = stdout
        self._returncode = returncode
        self._duration = duration

    @property
    def command(self):
        return self._command

    @property
    def stdout(self):
        return self._stdout

    @property
    def returncode(self):
        return self._returncode

    @property
    def duration(self):
        return self._duration

    def __str__(self):
        str = "{'command' : '%s', 'stdout': '%s', 'returncode' : '%s', 'duration' : '%s'}" \
            % (self.command, self.stdout, self.returncode, self.duration)
        return str


class CommandLineWrapper(object):

    def __init__(self, logger):
        self._is_open = False
        self._wrapper = None
        self._logger = logger
    
    @classmethod
    def init_as_subprocess_wrapper(self, logger):
        ''' Return a new SubprocessCommandLineWrapper.
        '''
        clw = CommandLineWrapper(logger)
        clw._wrapper = SubprocessCommandLineWrapper()
        return clw

    @classmethod
    def init_as_ssh_wrapper(self, logger, host, port=22, username=None, 
                            password=None, userkey=None):
        ''' Return a new SSHCommandLineWrapper.
        '''
        clw = CommandLineWrapper(logger)
        clw._wrapper = SSHCommandLineWrapper(host, port, username, password,
                                             userkey)
        return clw

    @classmethod
    def init_as_gsissh_wrapper(self, logger):
        ''' Return a new GSISSHCommandLineWrapper.
        '''
        clw = CommandLineWrapper(logger)
        clw._wrapper = GSISSHCommandLineWrapper()
        return clw

    def open(self):
        if self._is_open is True:
            raise CLWException("%s has already been opened." 
                % self._wrapper.__class__.__name__)
        else:
            try:
                self._is_open = True
                self._wrapper.open()
            except Exception, ex:
                raise CLWException('%s - %s' % (self._wrapper.__class__.__name__, ex))

    def close(self):
        if self._is_open is False:
            raise CLWException("%s is not in 'open' state." 
                % self._wrapper.__class__.__name__)
        else:
            try:
                self._wrapper.close()
                self._is_open = False
            except Exception, ex:
                raise CLWException('%s - %s' % (self._wrapper.__class__.__name__, ex))

    def run_sync(self, executable, arguments=[], environemnt={}):
        if self._is_open == False:
            raise CLWException("%s is not in 'open' state." 
                % self._wrapper.__class__.__name__)
        else:
            try:
                (cmd, stdout, rc, duration) = self._wrapper.run_sync(executable, arguments, environemnt)
                return CommandWrapperResult(cmd, stdout, rc, duration)
            except Exception, ex:
                raise CLWException('%s - %s' % (self._wrapper.__class__.__name__, ex))

    def run_async(self, executable, arguments=[], environemnt={}):
        raise Exception('Not Implemented')

