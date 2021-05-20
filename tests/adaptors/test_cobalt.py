#!/usr/bin/env python

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

"""
Tests for the Cobalt script generator function as well as the Cobalt adaptor.
"""

from unittest import mock

import radical.saga     as rs

from radical.saga.adaptors.cobalt import cobaltjob as rsacj

THETA_PPN = 64


# ------------------------------------------------------------------------------
#
def test_cobaltscript_generator():

    jd  = rs.job.Description()

    jd.name                = 'Test'
    jd.executable          = '/bin/sleep'
    jd.arguments           = 60
    jd.environment         = {'test_env': 15}
    jd.working_directory   = '/home/user'
    jd.output              = 'output.log'
    jd.error               = 'error.log'
    jd.processes_per_host  = THETA_PPN
    jd.number_of_processes = 4
    jd.queue               = 'normal-queue'
    jd.project             = 'TestProject'
    jd.wall_time_limit     = 70
    jd.system_architecture = {'options': ['mcdram=cache',
                                          'numa=quad',
                                          'mig-mode=True']}
    jd.total_cpu_count     = THETA_PPN * 2

    tgt_script = """
#!/bin/bash
#COBALT --jobname Test
#COBALT --cwd /home/user
#COBALT --output /home/user/output.log
#COBALT --error /home/user/error.log
#COBALT --time 01:10:00
#COBALT --queue normal-queue
#COBALT --project TestProject
#COBALT --nodecount 2
#COBALT --proccount 4
#COBALT --attrs mcdram=cache:numa=quad:mig-mode=True
#COBALT --env test_env=15
#COBALT --env SAGA_PPN=64

/bin/sleep 60

"""

    script = rsacj._cobaltscript_generator(url=None, logger=mock.Mock(),
                                           jd=jd, ppn=None)

    assert (script == tgt_script)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_cobaltscript_generator()

# ------------------------------------------------------------------------------
