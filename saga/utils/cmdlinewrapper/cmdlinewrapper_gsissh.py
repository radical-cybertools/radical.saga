# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a CommandLineWrapper implementation based on simple GSISSH 
    tunneling via a modified pexssh library.
'''

import time
from saga.utils.which import which
from pxgsissh import SSHConnection

class GSISSHCommandLineWrapper(object):

    def __init__(self, host, port, username, userproxies):
        ''' Create a new wrapper instance.
        '''
        self.host = host
        self.port = port
        self.userproxies = userproxies
        self.username = username

        self._connection = None

    def open(self):
        gsissh_executable = which('gsissh')
        if gsissh_executable is None:
            raise Exception("Couldn't find 'gsissh' executable in path.")

        self._connection = SSHConnection(executable=gsissh_executable, gsissh=True)
        self._connection.login(hostname=self.host, port=self.port,
                               username=self.username, password=None)

    def close(self):
        self._connection.logout()

    def run_sync(self, executable, arguments, environemnt):
        job_error = None
        job_output = None
        returncode = None

        cmd = executable
        for arg in arguments:
            cmd += " %s " % (arg)

        t1 = time.time()
        result = self._connection.execute(cmd)
        tdelta = time.time() - t1

        return (cmd, result['output'], result['exitcode'], tdelta)
