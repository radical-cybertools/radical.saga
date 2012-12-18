# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Unit tests for saga.engine.engine.py
"""

import os, sys
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

def test_emtpy_registry():
    """ Test that an empty adaptor registry is handled properly.
    """
    Engine()._load_adaptors([])
    assert Engine().loaded_adaptors() == {}

def test_broken_registry():
    """ Test that an attemt to load from a broken registry 
        is handled properly.
    """
    try:
        Engine()._load_adaptors(12)
        assert False
    except TypeError:
        assert True

def test_load_nonexistent_adaptor():
    """ Test that an attempt to load a non-existent adaptor is handled properly.
    """
    Engine()._load_adaptors(["nonexsitent"])
    assert len(Engine().loaded_adaptors()) == 0


def test_load_adaptor():
    """ Test that an attempt to load an adaptor is handled properly.
    """
    # store old sys.path
    old_sys_path = sys.path
    path = os.path.split(os.path.abspath(__file__))[0]
    sys.path.append(path)

    Engine()._load_adaptors(["mockadaptor_enabled"])
    assert len(Engine().loaded_adaptors()) == 1
    for (key, value) in Engine().loaded_adaptors().iteritems():
        assert key == 'saga.job.Job'
        # make sure the configuration gets passed through
        assert value['mock'][0]().get_config()["foo"].as_dict() == {'foo': 'bar'}

    # restore sys.path
    sys.path = old_sys_path

def test_load_broken_adaptor():
    """ Test that an expection in the adaptor register() method is handled properly.
    """
    # store old sys.path
    old_sys_path = sys.path
    path = os.path.split(os.path.abspath(__file__))[0]
    sys.path.append(path)

    Engine()._load_adaptors(["mockadaptor_broken"])
    assert len(Engine().loaded_adaptors()) == 0

    # restore sys.path
    sys.path = old_sys_path


