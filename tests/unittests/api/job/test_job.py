
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import saga
import saga.utils.test_config as sutc

from copy import deepcopy


# ------------------------------------------------------------------------------
#
def _silent_cancel(job_obj):
    # try to cancel job but silently ignore all errors
    try:
        job_obj.cancel()
    except Exception:
        pass


# ------------------------------------------------------------------------------
#
def _silent_close_js(js_obj):
    # try to cancel job but silently ignore all errors
    try:
        js_obj.close()
    except Exception:
        pass


# ------------------------------------------------------------------------------
#
def test_job_service_get_url():
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        assert js, "job service creation failed?"
        assert (tc.js_url == str(js.url)), "%s == %s" % (tc.js_url, str(js.url))

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_service_invalid_url():
    """ Test if a non-resolvable hostname results in a proper exception
    """
    try:
        tc = sutc.TestConfig()
        invalid_url = deepcopy(saga.Url(tc.js_url))
        invalid_url.host = "does.not.exist"
        tmp_js = saga.job.Service(invalid_url, tc.session)
        _silent_close_js(tmp_js)
        assert False, "Expected XYZ exception but got none."

    except saga.BadParameter:
        assert True
    except saga.SagaException as ex:
        assert False, "Expected BadParameter exception, but got %s" % ex


# ------------------------------------------------------------------------------
#
def test_job_service_create():
    """ Test service.create_job() - expecting state 'NEW'
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j1 = js.create_job(jd)
        assert j1.state == j1.get_state()
        assert j1.state == saga.job.NEW

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_run():
    """ Test job.run() - expecting state: RUNNING/PENDING
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j1 = js.create_job(jd)

        j1.run()

        assert (j1.state in [saga.job.RUNNING, saga.job.PENDING])

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j1)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_wait():
    """ Test job.wait() - expecting state: DONE (this test might take a while)
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j1 = js.create_job(jd)

        j1.run()
        j1.wait()
        assert j1.state == saga.job.DONE, "%s != %s" % (j1.state, saga.job.DONE)

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j1)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_multiline_run():
    """ Test job.run() with multiline command
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sh'
        jd.arguments = ["""-c "python -c '
import time
if True :
  if True :
    time.sleep (3)
'
"
"""]

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)
        j1 = js.create_job(jd)

        j1.run()
        assert (j1.state in [saga.job.RUNNING, saga.job.PENDING])
        j1.wait()
        assert (j1.state == saga.job.DONE), "%s == %s" % (j1.state, saga.job.DONE)

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j1)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_suspend_resume():
    """ Test job.suspend()/resume() - expecting state: SUSPENDED/RUNNING
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j1 = js.create_job(jd)
        j1.run()

        j1.suspend()
        assert j1.state == saga.job.SUSPENDED
        assert j1.state == j1.get_state()

        j1.resume()
        assert j1.state == saga.job.RUNNING
        assert j1.state == j1.get_state()

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j1)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_cancel():
    """ Test job.cancel() - expecting state: CANCELED
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j1 = js.create_job(jd)

        j1.run()
        j1.cancel()
        assert j1.state == saga.job.CANCELED

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_get_exit_code():
    """ Test job.exit_code
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = "/bin/sleep"

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)
        j.run()
        j.wait()

        ec = j.exit_code
        assert ec == 1, "%s != 1" % ec

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_get_service_url():
    """ Test if job.service_url == Service.url
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)

        assert j.service_url == js.url

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_get_id():
    """ Test job.get_id() / job.id
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)
        j.run()

        assert j.id is not None
        assert j.id == j.get_id()

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j)
        _silent_close_js(js)
