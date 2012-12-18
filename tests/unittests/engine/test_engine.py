# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Unit tests for saga.engine.engine.py
"""

from saga.engine import *



def test_singleton():
    """ Test that the object behaves like a singleton.
    """
    # make sure singleton works
    assert(getEngine() == getEngine())
    assert(getEngine() == Engine())

    e1 = Engine()
    e2 = Engine()
    assert(e1 == e2)

def test_configurable():
    """ Test the object's Configurable interface.
    """
    # make sure singleton works
    assert Engine().get_config()['foo'].get_value() == 'bar'  

def test_emtpy_regsitry():
    """ Test that an empty adaptor registry is handled properly.
    """
    Engine()._load_adaptors([])
    assert Engine().loaded_adaptors() == {}

def test_load_nonexisting_module():
    """ Test that an attemt to load a non-existing adaptor 
        is handled properly.
    """
    try:
        Engine()._load_adaptors(12)
        assert False
    except TypeError:
        assert True

    Engine()._load_adaptors([".mockadaptor"])
    assert Engine().loaded_adaptors() == {}


