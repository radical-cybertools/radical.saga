
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


__author__    = ["Ole Weidner", "Andre Merzky"]
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import saga
import saga.utils.test_config as sutc


# ------------------------------------------------------------------------------
#
def test_get_url():
    """ Testing job service url/get_url()
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


# ------------------------------------------------------------------------------
#
def test_list_jobs():
    """ Testing if a submitted job shows up in Service.list() """
    try:
        tc = sutc.TestConfig()

        # create job service and job
        js = saga.job.Service(tc.js_url, tc.session)
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

# ------------------------------------------------------------------------------
#
def test_run_job () :
    """ submit a job via run_job, and retrieve id """
    try:
        tc = sutc.TestConfig ()

        # create job service and job
        js = saga.job.Service (tc.js_url, tc.session)
        j  = js.run_job ("/bin/sleep 10")
        assert j.id

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se

# ------------------------------------------------------------------------------
#
def test_get_job () :
    """ submit a job, and retrieve it by id """
    try:
        tc = sutc.TestConfig ()

        # create job service and job
        js = saga.job.Service (tc.js_url, tc.session)
        jd = saga.job.Description ()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['10']

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
