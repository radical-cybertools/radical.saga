
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import pytest
import saga

import radical.utils as ru

# ------------------------------------------------------------------------------
def pytest_addoption (parser) :
    parser.addoption ("--configs", action="append", default=[],
        help="list of configs to use for testing")

# ------------------------------------------------------------------------------
def pytest_generate_tests (metafunc) :
    if 'configs' in metafunc.fixturenames :
        metafunc.parametrize ("configs",
                              metafunc.config.option.configs)

# ------------------------------------------------------------------------------
@pytest.fixture
def cfg (configs) :

    fname = "./configs/%s" % configs

    if  not os.path.exists (fname) :
        raise Exception ("no such config: '%s'" % fname)

    cfg = ru.read_json_str (fname)

    assert    ('saga.tests' in cfg)
    return cfg['saga.tests']



# ------------------------------------------------------------------------------
@pytest.fixture
def session (cfg) :

    s = saga.Session ()
    t = cfg.get ('context_tye')

    if  t :
        c = saga.Context (t)

        c.context_user_proxy = cfg.get ('context_user_proxy')
        c.context_user_id    = cfg.get ('context_user_id')
        c.context_user_pass  = cfg.get ('context_user_pass')

        s.add_context (c)

    return s

# ------------------------------------------------------------------------------

