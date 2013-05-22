
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import time
import saga
import saga.utils.test_config as sutc

from copy import deepcopy


# ------------------------------------------------------------------------------
#
def _silent_cancel(job_obj):
    # try to cancel job but silently ignore all errors
    try:
        print "Silent cancel for  %s (%s)" % (job_obj.id, job_obj.state)
        job_obj.cancel()
        print "Silent cancel done %s (%s)" % (job_obj.id, job_obj.state)
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
    try:
        print "test job.run 0"
        tc = sutc.TestConfig()
        print "test job.run 1"
        js = saga.job.Service(tc.js_url, tc.session)
        print "test job.run 2"
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)
        jd._attributes_dump ()
        print "test job.run 3"

        j = js.create_job(jd)
        print "test job.run 4 %s (%s) [%s]" % (j.id, j.state, time.time())

        j.run()
        print "test job.run 5 %s (%s) [%s]" % (j.id, j.state, time.time())

        assert (j.state in [saga.job.RUNNING, saga.job.PENDING])

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        print "test job.run finally"
        _silent_cancel(j)
        _silent_close_js(js)
        print "test job.run finally done"


# ------------------------------------------------------------------------------
#
def test_job_wait():
    """ Test job.wait() - expecting state: DONE (this test might take a while)
    """
    try:
        print "test job.wait 0"
        tc = sutc.TestConfig()
        print "test job.wait 1"
        js = saga.job.Service(tc.js_url, tc.session)
        print "test job.wait 2"
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)
        jd._attributes_dump ()
        print "test job.wait 3"

        j = js.create_job(jd)
        print "test job.wait 3 %s" % j.id

        j.run()
        print "test job.wait 4 %s (%s) [%s]" % (j.id, j.state, time.time())
        j.wait()
        print "test job.wait 5 %s (%s) [%s]" % (j.id, j.state, time.time())
        assert j.state == saga.job.DONE, "%s != %s" % (j.state, saga.job.DONE)
        print "test job.wait 6 %s (%s)" % (j.id, j.state)

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        print "test job.wait finally"
        _silent_cancel(j)
        _silent_close_js(js)
        print "test job.wait finally done"


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
        j = js.create_job(jd)

        j.run()
        assert (j.state in [saga.job.RUNNING, saga.job.PENDING])
        j.wait()
        assert (j.state == saga.job.DONE), "%s == %s" % (j.state, saga.job.DONE)

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
        print "test job.get_id 0"
        tc = sutc.TestConfig()
        print "test job.get_id 1"
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)
        jd._attributes_dump ()
        print "test job.get_id 3"

        j = js.create_job(jd)
        print "test job.get_id 4 %s (%s)" % (j.id, j.state)
        j.run()
        print "test job.get_id 5 %s (%s)" % (j.id, j.state)
        print "test job.get_id 6 %s (%s)" % (j.get_id(), j.state)

        assert j.id is not None
        assert j.id == j.get_id()

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        print "test job.get_id finally"
        _silent_cancel(j)
        _silent_close_js(js)
        print "test job.get_id finally done"

