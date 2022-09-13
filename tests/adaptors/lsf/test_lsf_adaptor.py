#!/usr/bin/env python

__author__    = 'Ioannis Paraskevakos'
__copyright__ = 'Copyright 2018-2019, The SAGA Project'
__license__   = 'MIT'


'''
This test tests the LSF script generator function as well as the LSF adaptor
'''

import radical.saga     as rs
import radical.saga.url as rsurl

from radical.saga.adaptors.lsf.lsfjob import _lsfscript_generator


# ------------------------------------------------------------------------------
#
def test_lsfscript_generator():

    url = rsurl.Url('gsissh://summit.ccs.ornl.gov')
    jd  = rs.job.Description()

    jd.name                = 'Test'
    jd.executable          = '/bin/sleep'
    jd.arguments           = 60
    jd.environment         = {'test_env': 15, 'RADICAL_SMT': 2}
    jd.output              = 'output.log'
    jd.error               = 'error.log'
    jd.queue               = 'normal-queue'
    jd.project             = 'TestProject'
    jd.wall_time_limit     = 70
    jd.system_architecture = {'smt': 2,
                              'options': ['gpumps', 'nvme']}
    jd.total_cpu_count     = 65 * jd.system_architecture['smt']

    tgt_script = '\n#!/bin/bash \n' \
               + '#BSUB -q normal-queue \n' \
               + '#BSUB -J Test \n' \
               + '#BSUB -W 1:10 \n' \
               + '#BSUB -o output.log \n' \
               + '#BSUB -e error.log \n' \
               + '#BSUB -P TestProject \n' \
               + '#BSUB -nnodes 2 \n' \
               + "#BSUB -alloc_flags 'gpumps nvme smt2' \n" \
               + '\n' \
               + 'export test_env=15 RADICAL_SMT=2\n' \
               + '/bin/sleep 60'

    script = _lsfscript_generator(url=url, logger=None, jd=jd,
                                  ppn=None, lsf_version=None, queue=None)

    assert (script == tgt_script)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_lsfscript_generator()


# ------------------------------------------------------------------------------

