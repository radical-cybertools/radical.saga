# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Unit tests for saga.engine.engine.py
"""

import saga.cpi.base
import saga.cpi.job

_adaptor_info = [{'name'    : 'saga.adaptor.mock',
                  'type'    : 'saga.job.Job',
                  'class'   : 'MockJob',
                  'schemas' : ['fork', 'local']
                 }]

def register():
    return _adaptor_info

class MockJob(saga.cpi.job.Job):
    def __init__ (self) :
        saga.cpi.Base.__init__ (self, _adaptor_info['name'])
        print "local job adaptor init";

