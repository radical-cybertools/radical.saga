
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import saga
import saga.utils.test_config as sutc


# ------------------------------------------------------------------------------
#
def _silent_cancel(job_obj):
    # try to cancel job but silently ignore all errors
    try:
        print "silent cancel for  %s (%s)" % (job_obj.id, job_obj.state)
        job_obj.cancel()
        print "silent cancel done %s (%s)" % (job_obj.id, job_obj.state)
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
def test_close():
    """ Test job service close()
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
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
    try:
        tc = sutc.TestConfig()

        for i in range(0, 100):
            js = saga.job.Service(tc.js_url, tc.session)
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
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        assert str(js.get_url()) == str(tc.js_url)
        assert str(js.url) == str(tc.js_url)

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
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)

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
    """ Test to submit a job via run_job, and retrieve id """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)

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
    try:
        print "test get job 0"
        tc = sutc.TestConfig()
        print "test get job 1"
        js = saga.job.Service(tc.js_url, tc.session)

        # create job service and job
        print "test get job 2"
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        print "test get job 3 %s" % jd
        jd._attributes_dump ()
        j = js.create_job(jd)

        # run job - now it has an id, and js must be able to retrieve it by id
        print "test get job 3"
        j.run()
        print "test get job 4 %s" % j.id
        j_clone = js.get_job(j.id)
        print "test get job 5 %s" % j_clone.id
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
    print "helper multiple services 0"
    tc = sutc.TestConfig()
    print "helper multiple services 1"
    js = saga.job.Service(tc.js_url, tc.session)
    print "helper multiple services 2"
    jd = saga.job.Description()
    print "helper multiple services 3"
    jd.executable = '/bin/sleep'
    print "helper multiple services 4"
    jd.arguments = ['10']
    print "helper multiple services 5"
    jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)
    print "helper multiple services 6"
    j = js.create_job(jd)
    print "helper multiple services 7"
    j.run()
    print "helper multiple services 8"
    assert (j.state in [saga.job.RUNNING, saga.job.PENDING]), "job submission failed"
    print "helper multiple services 9"
    _silent_cancel(j)
    print "helper multiple services 10"
    _silent_close_js(js)
    print "helper multiple services 11"


# ------------------------------------------------------------------------------
#
def test_multiple_services():
    """ Test to create multiple job service instances  (this test might take a while) """
    try:
        tc = sutc.TestConfig()
        for i in range(0, 20):
            print " ------------------------ %s" % i
            helper_multiple_services(i)
            print " ------------------------ %s ok" % i

    except saga.NotImplemented as ni:
        assert tc.notimpl_warn_only, "%s " % ni
        if tc.notimpl_warn_only:
            print "%s " % ni

    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
