
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import time
import saga

import radical.utils.testing  as testing
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
    """ Test if the job service URL is returned cotrrectly
    """
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        assert js, "job service creation failed?"
        assert (tc.job_service_url == str(js.url)), "%s == %s" % (tc.job_service_url, str(js.url))

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
        tc = testing.get_test_config ()
        invalid_url = deepcopy(saga.Url(tc.job_service_url))
        invalid_url.host = "does.not.exist"
        tmp_js = saga.job.Service(invalid_url, tc.session)
        _silent_close_js(tmp_js)
        assert False, "Expected XYZ exception but got none."

    except saga.BadParameter :
        assert True

    # we don't check DNS anymore, as that can take *ages* -- so we now also
    # see Timeout and NoSuccess exceptions...
    except saga.Timeout :
        assert True

    except saga.NoSuccess :
        assert True

    # other exceptions sould never occur
    except saga.SagaException as ex:
        assert False, "Expected BadParameter, Timeout or NoSuccess exception, but got %s" % ex


# ------------------------------------------------------------------------------
#
def test_job_service_create():
    """ Test service.create_job() - expecting state 'NEW'
    """
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)
        assert j.state == j.get_state()
        assert j.state == saga.job.NEW

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
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)

        j.run()

        assert (j.state in [saga.job.RUNNING, saga.job.PENDING]), "j.state: %s" % j.state

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
def test_job_wait():
    """ Test job.wait() - expecting state: DONE (this test might take a while)
    """
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)

        j.run()
        j.wait()
        assert j.state == saga.job.DONE, "%s != %s" % (j.state, saga.job.DONE)

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
def test_job_multiline_run():
    """ Test job.run() with multiline command
    """
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
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
        j = js.create_job(jd)

        j.run()
        assert (j.state in [saga.job.RUNNING, saga.job.PENDING]), 'j.state: %s' % j.state
        j.wait()
        assert (j.state == saga.job.DONE), "j.state: %s " % j.state

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
def test_job_suspend_resume():
    """ Test job.suspend()/resume() - expecting state: SUSPENDED/RUNNING
    """
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)
        j.run()

        j.suspend()
        assert j.state == saga.job.SUSPENDED
        assert j.state == j.get_state()

        j.resume()
        assert j.state == saga.job.RUNNING
        assert j.state == j.get_state()

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
def test_job_cancel():
    """ Test job.cancel() - expecting state: CANCELED
    """
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)

        j.run()
        j.cancel()
        assert j.state == saga.job.CANCELED

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
def test_job_run_many():
    """ Run a bunch of jobs concurrently via the same job service.
    """
    NUM_JOBS = 32

    js   = None
    jobs = []
    try:

        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['60']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        for i in range(0, NUM_JOBS):
            j = js.create_job(jd)
            jobs.append(j)

        # start all jobs
        for job in jobs:
            job.run()

        # wait a bit
        time.sleep(10)

        for job in jobs:
            job.cancel()
            assert job.state == saga.job.CANCELED

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        for j in jobs:
            _silent_cancel(j)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_get_exit_code():
    """ Test job.exit_code
    """
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
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
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
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
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
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

