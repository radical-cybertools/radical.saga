
__author__    = 'Radical.Utils Development Team'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import os
import pytest

import radical.saga  as rs
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def pytest_addoption(parser):
    parser.addoption('--configs', action='append', default=[],
        help='list of configs to use for testing')


# ------------------------------------------------------------------------------
#
def pytest_generate_tests(metafunc):
    if 'configs' in metafunc.fixturenames:
        metafunc.parametrize('configs',
                              metafunc.config.option.configs,
                              scope='module')


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope='module')
def cfg(configs):

    fname = './configs/%s' % configs

    if  not os.path.exists(fname):
        raise Exception('no such config [%s]' % fname)

    cfg = ru.read_json_str(fname)

    assert    ('rs.tests' in cfg)
    return cfg['rs.tests']


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope='module')
def session(cfg):

    s = rs.Session()
    t = cfg.get('context_tye')

    if  t:
        c = rs.Context(t)

        c.context_user_proxy = cfg.get('context_user_proxy')
        c.context_user_id    = cfg.get('context_user_id')
        c.context_user_pass  = cfg.get('context_user_pass')

        s.add_context(c)

    return s


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope='module')
def config():

    # --------------------------------------------------------------------------
    #
    class Config(ru.TestConfig):

        # ----------------------------------------------------------------------
        #
        @staticmethod
        def configure_jd(cfg, jd):

            for key in ['job_walltime_limit' ,
                        'job_project'        ,
                        'job_queue'          ,
                        'job_total_cpu_count',
                        'job_spmd_variation' ]:
                jd.set_attribute(key, cfg.get(key))


        # ----------------------------------------------------------------------
        #
        @staticmethod
        def assert_exception(cfg, e):

            ni = cfg.get('not_implemented', 'warn')

            if 'NotImplemented' in str(e):
                if   ni == 'ignore': pass
                elif ni == 'warn'  : print 'WARNING: %s'
                else:
                    assert(False), 'unexpected exception: %s' % e


        # ----------------------------------------------------------------------
        #
        @staticmethod
        def silent_cancel(obj):

            if not isinstance(obj, list):
                obj = [obj]

            for o in obj:
                try:
                    o.cancel()
                except:
                    pass


    # --------------------------------------------------------------------------
    #
    return Config(ru.get_test_config())


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def job_service(session, cfg, request):

    assert ('job_service_url' in cfg)

    js = rs.job.Service(cfg['job_service_url'], session=session)

    def close():
        js.close()

    request.addfinalizer(close)

    return js


# ------------------------------------------------------------------------------

