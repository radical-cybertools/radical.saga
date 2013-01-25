
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a CommandLineWrapper implementation based on the Python
    subprocess pacakge.
'''

import time
import subprocess 

class SubprocessCommandLineWrapper(object):

    def __init__(self):
        pass

    def open(self):
        # no need to open. not a persistent connection
        pass

    def get_pipe (self) :
        return None

    def close(self):
        # no need to close. not a persistent connection
        pass

    def run_sync(self, executable, arguments, environemnt):
        job_error = None
        job_output = None
        returncode = None

        cmd = executable
        for arg in arguments:
            cmd += " %s " % (arg)

        t1 = time.time()
        pid = subprocess.Popen(cmd, shell=True, 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.PIPE)
        
        out, err = pid.communicate() 
        tdelta = time.time() - t1

        return (cmd, out, err, pid.returncode, tdelta)

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

