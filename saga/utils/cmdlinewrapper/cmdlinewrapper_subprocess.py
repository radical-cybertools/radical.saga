# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

''' Provides a CommandLineWrapper implementation based on the Python
    subprocess pacakge.
'''

import time
import subprocess 

class SubprocessCommandLineWrapper(object):
    pass

    def __init__(self):
        pass

    def open(self):
        # no need to open. not a persistent connection
        pass

    def close(self):
        # no need to close. not a persistent connection
        pass

    def run(self, executable, arguments, environemnt):
        job_error = None
        job_output = None
        returncode = None

        cmd = executable
        for arg in arguments:
            cmd += " %s " % (arg)

        t1 = time.time()
        pid = subprocess.Popen(cmd, shell=True, 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.STDOUT)
        
        out, err = pid.communicate() 
        tdelta = time.time() - t1

        return (cmd, out, pid.returncode, tdelta)
