#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

"""
Tests for the Slurm script generator function as well as the Slurm adaptor.
"""

import radical.saga  as rs
import radical.utils as ru

from unittest import mock

from radical.saga.adaptors.slurm import slurm_job

CORES_PER_NODE   = 56
JOB_MANAGER_ENDPOINT = 'slurm+ssh://frontera.tacc.utexas.edu/'


# ------------------------------------------------------------------------------
#
@mock.patch.object(slurm_job.SLURMJobService, '__init__', return_value=None)
@mock.patch.object(slurm_job.SLURMJobService, '_handle_file_transfers')
def test_slurm_generator(mocked_handle_ft, mocked_init):

    jd = rs.job.Description()

    jd.name                = 'TestSlurm'
    jd.executable          = '/bin/sleep'
    jd.arguments           = ['60']
    jd.environment         = {'test_env': 15, 'RADICAL_SMT': 2}
    jd.pre_exec            = ['echo $test_env']
    jd.post_exec           = ['echo $test_env']
    jd.working_directory   = '/home/user'
    jd.output              = 'output.log'
    jd.error               = 'error.log'

    jd.queue               = 'normal-queue'
    jd.project             = 'TestProject:ReservationTag'
    jd.wall_time_limit     = 70

    jd.processes_per_host  = CORES_PER_NODE
    jd.total_cpu_count     = CORES_PER_NODE * 2
    # jd.system_architecture = {'gpu': 'p100'}
    jd.system_architecture = {'smt'          : 2,
                              'options'      : ['nvme', 'intel'],
                              'blocked_cores': [0, 1]}

    # - blocked cores represent virtual cores
    #   (e.g., physical cores with 1 thread or threads, if physical cores
    #   have more than one thread per core)
    # - core specialization targets physical cores only
    #   (i.e., num_blocked_cores // smt)

    tgt_script = """#!/bin/sh

#SBATCH -N 2
#SBATCH -J "TestSlurm"
#SBATCH -D "/home/user"
#SBATCH --output "output.log"
#SBATCH --error "error.log"
#SBATCH --account "TestProject"
#SBATCH --partition "normal-queue"
#SBATCH --reservation "ReservationTag"
#SBATCH --constraint "nvme&intel"
#SBATCH --time 01:10:00
#SBATCH --core-spec=1
#SBATCH --threads-per-core=2

## ENVIRONMENT
export test_env="15"
export RADICAL_SMT="2"

## PRE_EXEC
echo $test_env

## EXEC
/bin/sleep 60

## POST_EXEC
echo $test_env
"""

    js = slurm_job.SLURMJobService(api=None, adaptor=None)
    js._adaptor = slurm_job.Adaptor()
    js._ppn     = 1  # `jd.processes_per_host` will be used instead
    js.rm       = ru.Url(JOB_MANAGER_ENDPOINT)
    js.jobs     = {}
    js._logger = js.shell = mock.Mock()

    job_id_part = '11111'
    script_cont = ''
    script_name = ''

    def get_slurm_script(src, tgt):
        nonlocal script_cont
        nonlocal script_name
        script_cont = src
        script_name = tgt

    js.shell.write_to_remote.side_effect = get_slurm_script
    js.shell.run_sync.return_value = \
        (0, 'Submitted batch job %s' % job_id_part, None)

    # generates and submits SLURM script
    job_id = js._job_run(job_obj=js.create_job(jd))

    assert (job_id_part in job_id)
    assert (script_name.endswith('.slurm'))
    assert (script_cont == tgt_script)

    assert (js.jobs[job_id]['state']  == rs.job.constants.PENDING)
    assert (js.jobs[job_id]['output'] == jd.output)
    assert (js.jobs[job_id]['error']  == jd.error)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    test_slurm_generator()

# ------------------------------------------------------------------------------
