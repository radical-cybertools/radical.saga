# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Unit tests for saga.engine.engine.py
'''

from saga.engine import *

############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    # make sure singleton works
    assert(getEngine() == getEngine())
    assert(getEngine() == Engine())

    e1 = Engine()
    e2 = Engine()
    assert(e1 == e2)

def test_configurable():
    # make sure singleton works
    assert Engine().get_config()['foo'].get_value() == 'bar'  
##
############################## END UNIT TESTS #################################