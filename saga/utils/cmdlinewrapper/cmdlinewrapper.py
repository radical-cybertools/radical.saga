
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides the base class and factory methods for the various command-line 
    wrapper, like GSISSH and SSH. 
'''

from saga.utils.exception      import ExceptionBase, get_traceback

from cmdlinewrapper_subprocess import SubprocessCommandLineWrapper
from cmdlinewrapper_gsissh     import GSISSHCommandLineWrapper
from cmdlinewrapper_ssh        import SSHCommandLineWrapper

# --------------------------------------------------------------------
#
class CLWException(ExceptionBase):
    ''' Raised for CommandLineWrapper exceptions.
    '''

# --------------------------------------------------------------------
#
class CommandWrapperResult(object):
    ''' A 5-tuple returned by CommandLineWrapper.run().
    '''
    # ----------------------------------------------------------------
    #
    def __init__(self, command, stdout=None, stderr=None, returncode=None, ttc=-1):

        self._command    = command
        self._stdout     = stdout
        self._stderr     = stderr
        self._returncode = returncode
        self._ttc        = ttc

    # ----------------------------------------------------------------
    #
    @property
    def command(self):
        return self._command

    # ----------------------------------------------------------------
    #
    @property
    def stdout(self):
        return self._stdout

    # ----------------------------------------------------------------
    #
    @property
    def stderr(self):
        return self._stderr

    # ----------------------------------------------------------------
    #
    @property
    def returncode(self):
        return self._returncode

    # ----------------------------------------------------------------
    #
    @property
    def ttc(self):
        return self._ttc

    # ----------------------------------------------------------------
    #
    def __str__(self):
        str = "{'command' : '%s', 'stdout': '%s', 'stderr' : %s, 'returncode' : '%s', 'ttc' : '%s'}" \
            % (self.command, self.stdout, self.stderr, self.returncode, self.ttc)
        return str


# --------------------------------------------------------------------
#
class CommandLineWrapper(object):

    # ----------------------------------------------------------------
    #
    def __init__(self, scheme='shell', host='localhost', port=None, 
                 username=None, password=None, userkeys=[], userproxies=[]):

        self._is_open = False
        self._wrapper = None

        if  scheme.lower() == "fork"    or \
            scheme.lower() == "shell"   or \
            scheme.lower() == "local"   or \
            scheme.lower() == "subprocess" :
            self._wrapper = SubprocessCommandLineWrapper()

        elif scheme.lower() == "ssh" :
            self._wrapper = SSHCommandLineWrapper   (host, port, username, password, userkeys)
    
        elif scheme.lower() == "gsissh" :
            self._wrapper = GSISSHCommandLineWrapper(host, port, userproxies)
    
        else :
            raise CLWException("scheme %s is not supported." % scheme)
            
    
    # ----------------------------------------------------------------
    #
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

    # ----------------------------------------------------------------
    #
    def get_pipe(self):
        return self._wrapper.get_pipe ()

    # ----------------------------------------------------------------
    #
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

    # ----------------------------------------------------------------
    #
    def run_sync(self, exe, args=[], env={}):
        if self._is_open == False:
            raise CLWException("%s is not in 'open' state." 
                % self._wrapper.__class__.__name__)
        else:
            try:
                return CommandWrapperResult(self._wrapper.run_sync(exe, args, env))
            except Exception, ex:
                print get_traceback ()
                raise CLWException('%s - %s' % (self._wrapper.__class__.__name__, ex))

    # ----------------------------------------------------------------
    #
    # def run_async(self, exe, args=[], env={}):
    #     raise Exception('Not Implemented')



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

