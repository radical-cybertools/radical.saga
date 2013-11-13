
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import saga
import radical.utils.testing  as testing
import saga.utils.test_config as sutc


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
        js_obj.close()
    except Exception:
        pass


# ------------------------------------------------------------------------------
#
def test_close():
    """ Test job service close()
    """
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        js.close()
        js.get_url()
        assert False, "Subsequent calls should fail after close()"

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException:
        assert True


# ------------------------------------------------------------------------------
#
def test_open_close():
    """ Test job service create / close() in a big loop
    """
    js = None
    try:
        tc = testing.get_test_config ()

        for i in range(0, 10):
            js = saga.job.Service(tc.job_service_url, tc.session)
            js.close()

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
def test_get_url():
    """ Test job service url/get_url()
    """
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        assert str(js.get_url()) == str(tc.job_service_url)
        assert str(js.url) == str(tc.job_service_url)

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
def test_list_jobs():
    """ Test if a submitted job shows up in Service.list() """
    j  = None
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)

        # create job service and job
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)

        # run job - now it has an id, and js must know it
        j.run()
        all_jobs = js.list()
        assert j.id in all_jobs, \
            "%s not in %s" % (j.id, all_jobs)

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
def test_run_job():
    """ Test to submit a job via run_job, and retrieve id"""
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)

        # create job service and job
        j = js.run_job("/bin/sleep 10")
        assert j.id

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
def test_get_job():
    """ Test to submit a job, and retrieve it by id """
    j  = None
    js = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)

        # create job service and job
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)

        # run job - now it has an id, and js must be able to retrieve it by id
        j.run()
        j_clone = js.get_job(j.id)
        assert j.id in j_clone.id

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
def helper_multiple_services(i):
    tc = testing.get_test_config ()
    js = saga.job.Service(tc.job_service_url, tc.session)
    jd = saga.job.Description()
    jd.executable = '/bin/sleep'
    jd.arguments = ['10']
    jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)
    j = js.create_job(jd)
    j.run()
    assert (j.state in [saga.job.RUNNING, saga.job.PENDING]), "job submission failed"
    _silent_cancel(j)
    _silent_close_js(js)


# ------------------------------------------------------------------------------
#
NUM_SERVICES = 20

def test_multiple_services():
    """ Test to create multiple job service instances  (this test might take a while) """
    try:
        tc = testing.get_test_config ()
        for i in range(0, NUM_SERVICES):
            helper_multiple_services(i)

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni

    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se

