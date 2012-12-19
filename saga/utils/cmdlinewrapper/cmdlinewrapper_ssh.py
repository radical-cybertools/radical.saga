# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a CommandLineWrapper implementation based on simple SSH 
    tunneling via a modified pexssh library.
'''

import time
from saga.utils.which import which
from pxgsissh import SSHConnection

class SSHCommandLineWrapper(object):

    def __init__(self, host, port, username, password, userkeys):
        ''' Create a new wrapper instance.
        '''
        self.host = host
        self.port = port
        self.password = password
        self.userkeys = userkeys
        self.username = username

        self._connection = None

    def open(self):
        ssh_executable = which('ssh')
        if ssh_executable is None:
            raise Exception("Couldn't find 'ssh' executable in path.")

        self._connection = SSHConnection(executable=ssh_executable, gsissh=False)
        self._connection.login(hostname=self.host, port=self.port,
                               username=self.username, password=self.password)


    def close(self):
        self._connection.logout()

    def run_sync(self, executable, arguments, environemnt):
        job_error = None
        job_output = None
        returncode = None

        cmd = executable
        for arg in arguments:
            cmd += " %s " % (arg)

        stderr = "/tmp/saga.cmd.stderr.%d"  %  id(self)
        cmd += " 2>%s"  %  stderr

        t1 = time.time()
        result = self._connection.execute(cmd)
        tdelta = time.time() - t1

        reserr = self._connection.execute("cat %s ; rm %s" %  (stderr, stderr))
        result['error'] = "error: %s" % reserr['output']

        return (cmd, result['output'], result['error'], result['exitcode'], tdelta)


