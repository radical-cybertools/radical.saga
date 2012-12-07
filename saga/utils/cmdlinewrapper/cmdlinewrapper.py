# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides the base class and factory methods for the various command-line 
    wrapper, like GSISSH and SSH. 
'''

from saga.utils.exception import ExceptionBase

from cmdlinewrapper_subprocess import SubprocessCommandLineWrapper
from cmdlinewrapper_gsissh import GSISSHCommandLineWrapper
from cmdlinewrapper_ssh import SSHCommandLineWrapper

class CLWException(ExceptionBase):
    ''' Raised for CommandLineWrapper exceptions.
    '''

class CommandWrapperResult(object):
    ''' A 4-tuple returned by CommandLineWrapper.run().
    '''
    def __init__(self, command, stdout=None, returncode=None, ttc=-1):

        self._command = command
        self._stdout = stdout
        self._returncode = returncode
        self._ttc = ttc

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
    def ttc(self):
        return self._ttc

    def __str__(self):
        str = "{'command' : '%s', 'stdout': '%s', 'returncode' : '%s', 'ttc' : '%s'}" \
            % (self.command, self.stdout, self.returncode, self.ttc)
        return str


class CommandLineWrapper(object):

    def __init__(self):
        self._is_open = False
        self._wrapper = None
    
    @classmethod
    def init_as_subprocess_wrapper(self):
        ''' Return a new SubprocessCommandLineWrapper.
        '''
        clw = CommandLineWrapper()
        clw._wrapper = SubprocessCommandLineWrapper()
        return clw

    @classmethod
    def init_as_ssh_wrapper(self, host, port=22, username=None, 
                            password=None, userkeys=[]):
        ''' Return a new SSHCommandLineWrapper.
        '''
        clw = CommandLineWrapper()
        clw._wrapper = SSHCommandLineWrapper(host, port, username, password,
                                             userkeys)
        return clw

    @classmethod
    def init_as_gsissh_wrapper(self, host, port=22, username=None, 
                               userproxies=[]):
        ''' Return a new GSISSHCommandLineWrapper.
        '''
        clw = CommandLineWrapper()
        clw._wrapper = GSISSHCommandLineWrapper(host, port, username, userproxies)
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

