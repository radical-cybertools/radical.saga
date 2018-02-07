
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for saga.engine.engine.py
"""

import os, sys
from   radical.saga.engine.engine import Engine

import radical.utils as ru


def test_singleton():
    """ Test that the object behaves like a singleton
    """
    # make sure singleton works
    assert(Engine() == Engine())

    e1 = Engine()
    e2 = Engine()
    assert(e1 == e2)

def test_configurable():
    """ Test the object's Configurable interface
    """
    # make sure singleton works
    assert(not Engine()._cfg['load_beta_adaptors'])

def test_emtpy_registry():
    """ Test that an empty adaptor registry is handled properly
    """
    Engine()._load_adaptors([])
    assert Engine().loaded_adaptors() == {}

def test_broken_registry():
    """ Test that an attemt to load from a broken registry 
        is handled properly
    """
    try:
        Engine()._load_adaptors(12)
        assert False
    except TypeError:
        assert True

def test_load_nonexistent_adaptor():
    """ Test that an attempt to load a non-existent adaptor is handled properly
    """
    Engine()._load_adaptors(["nonexistent"])
    assert len(Engine().loaded_adaptors()) == 0

def test_load_adaptor():
    """ Test that an attempt to load an adaptor is handled properly
    """
    # store old sys.path
    old_sys_path = sys.path
    path = os.path.split(os.path.abspath(__file__))[0]
    sys.path.append(path)

    Engine()._load_adaptors(["mockadaptor_enabled"])
    # make sure the adapor is in the list
    assert(len(Engine().loaded_adaptors()['radical.saga.job.Job']['mock']) == 1), \
            pprint.pformat(Engine().loaded_adaptors())

    # make sure the configuration gets passed through
    cpis         = Engine().loaded_adaptors()
    adaptor      = cpis['radical.saga.job.Job']['mock'][0]['adaptor_instance']

    # restore sys.path
    sys.path = old_sys_path

def test_load_adaptor_twice():
    """ Test that an attempt to load the same adaptor twice doesn't cause trouble """
    # store old sys.path
    old_sys_path = sys.path
    path = os.path.split(os.path.abspath(__file__))[0]
    sys.path.append(path)

    Engine()._load_adaptors(["mockadaptor_enabled", "mockadaptor_enabled"])
    cpis  = Engine().loaded_adaptors()
    mocks = cpis['radical.saga.job.Job']['mock']
    assert len(mocks) == 1

    # make sure the configuration gets passed through
    cpis         = Engine().loaded_adaptors()
    adaptor      = cpis['radical.saga.job.Job']['mock'][0]['adaptor_instance']

    # restore sys.path
    sys.path = old_sys_path

def test_load_broken_adaptor():
    """ Test that an expection in the adaptor's sanity_check() method is handled properly
    """
    # store old sys.path
    old_sys_path = sys.path
    path = os.path.split(os.path.abspath(__file__))[0]
    sys.path.append(path)

    Engine()._load_adaptors(["mockadaptor_broken"])
    assert len(Engine().loaded_adaptors()) == 0

    # restore sys.path
    sys.path = old_sys_path




