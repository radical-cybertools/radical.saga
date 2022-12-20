#!/usr/bin/env python

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

"""
Tests for the PBSPro script generator function as well as the PBSPro adaptor.
"""

from unittest import mock

import radical.saga as rs

from radical.saga.adaptors.pbspro import pbsprojob as rsapj

JOB_MANAGER_ENDPOINT = 'pbspro://polaris.alcf.anl.gov/'
POLARIS_PPN = 64
NUM_NODES   = 4


# ------------------------------------------------------------------------------
#
def test_pbsproscript_generator():

    jd  = rs.job.Description()

    jd.name                = 'Test'
    jd.executable          = '/bin/sleep'
    jd.arguments           = 10
    jd.environment         = {'test_env': 15, 'RADICAL_BASE': '/tmp'}
    jd.working_directory   = '/home/user'
    jd.output              = 'output.log'
    jd.error               = 'error.log'
    jd.processes_per_host  = POLARIS_PPN
    jd.queue               = 'normal-queue'
    jd.project             = 'PROJ0000'
    jd.wall_time_limit     = 15
    jd.system_architecture = {'options': ['filesystems=grand:home',
                                          'place=scatter']}
    jd.total_cpu_count     = POLARIS_PPN * NUM_NODES
    jd.total_gpu_count     = NUM_NODES

    tgt_script = """
#!/bin/bash

#PBS -N Test
#PBS -o /home/user/output.log
#PBS -e /home/user/error.log
#PBS -l walltime=0:15:00
#PBS -A PROJ0000
#PBS -q normal-queue
#PBS -l select=4:ncpus=64
#PBS -l filesystems=grand:home
#PBS -l place=scatter
#PBS -v \\"test_env=15\\",\\"RADICAL_BASE=/tmp\\"

export SAGA_PPN=64
export PBS_O_WORKDIR=/home/user 
mkdir -p /home/user
cd       /home/user

/bin/sleep 10
"""

    script = rsapj._script_generator(url=None, logger=mock.Mock(), jd=jd, ppn=1,
                                     gres=None, version='', is_cray=False,
                                     queue=None)

    assert (script == tgt_script)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_pbsproscript_generator()

# ------------------------------------------------------------------------------
