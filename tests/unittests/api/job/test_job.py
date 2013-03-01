__author__    = ["Ole Weidner", "Andre Merzky"]
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import os
import sys
import saga
import saga.utils.test_config as sutc

from copy import deepcopy

long_job = None


# # ------------------------------------------------------------------------------
# #
# def test_job_invalid_session():
#     """ Testing if an invalid session results in a proper exception
#     """
#     try:
#         tc = sutc.TestConfig()
#         # generate an invalid session
#         valid_session   = tc.session
#         invalid_session = saga.Session(default=False)
# 
#         for valid_context in valid_session.contexts :
#             invalid_context = saga.Context (valid_context.type)
# 
#             for a in valid_context.list_attributes () :
#                 invalid_context.set_attribute (a, valid_context.get_attribute (a) + "-hahaha")
# 
#             invalid_session.add_context (invalid_context)
# 
#         js = saga.job.Service(tc.js_url, invalid_session)
#         assert False, "Expected XYZ exception but got none."
# 
#     except saga.BadParameter:
#         assert True
#     except saga.SagaException as ex:
#         assert False, "Expected BadParameter exception, but got %s" % ex
# 

# ------------------------------------------------------------------------------
#
def test_job_service_invalid_url():
    """ Testing if a non-resolvable hostname results in a proper exception
    """
    try:
        tc = sutc.TestConfig()
        invalid_url       = deepcopy(saga.Url(tc.js_url))
        invalid_url.host += ".does.not.exist"
        js = saga.job.Service(invalid_url, tc.session)
        assert False, "Expected XYZ exception but got none."

    except saga.BadParameter:
        assert True
    except saga.SagaException as ex:
        assert False, "Expected BadParameter exception, but got %s" % ex
        
# ------------------------------------------------------------------------------
#
def test_job_service_create():
    """ Testing service.create_job() - expecting state 'NEW'
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j1 = js.create_job(jd)
        assert j1.state == j1.get_state()
        assert j1.state == saga.job.NEW

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
# ------------------------------------------------------------------------------
#
def test_job_run():
    """ Testing job.run() - expecting state: RUNNING/PENDING
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j1 = js.create_job(jd)

        j1.run()
        assert (j1.state in [saga.job.RUNNING, saga.job.PENDING])
        assert j1.state == j1.get_state()

        global long_job
        long_job = j1

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se


# ------------------------------------------------------------------------------
#
def test_job_suspend_resume():
    """ Testing job.suspend()/resume() - expecting state: SUSPENDED/RUNNIG
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j1 = js.create_job(jd)
        j1.run()

        j1.suspend()
        assert j1.state == saga.job.SUSPENDED
        assert j1.state == j1.get_state()

        j1.resume()
        assert j1.state == saga.job.RUNNING
        assert j1.state == j1.get_state()

        j1.cancel()

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se


# ------------------------------------------------------------------------------
#
def test_job_cancel():
    """ Testing job.cancel() - expecting state: CANCELED
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

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


# ------------------------------------------------------------------------------
#
def test_job_states_OLD():

    return 0

    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['3']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j3 = js.run_job ("/bin/sleep 3 ; /bin/true")
        assert j3.state == saga.job.RUNNING
                         
        j3.wait ()       
        assert j3.state == saga.job.DONE

        j4 = js.run_job ("/bin/sleep 3 ; /bin/false")
        assert j4.state == saga.job.RUNNING
                         
        j4.wait ()       
        assert j4.state == saga.job.FAILED

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se


# ------------------------------------------------------------------------------
#
def test_get_exit_code():
    """ Testing job.exit_code
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)

        jd = saga.job.Description()
        jd.executable = "/bin/sh"
        jd.arguments = ["-c \"exit 3\""]

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j = js.create_job(jd)
        j.run()
        j.wait()

        ec = j.exit_code
        assert ec == 3, "%s != 3" % ec

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se


# ------------------------------------------------------------------------------
#
def test_get_service_url():
    """ Testing if job.service_url == Service.url
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)

        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j = js.create_job(jd)

        assert j.service_url == js.url

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se


# ------------------------------------------------------------------------------
#
def test_get_id():
    """ Testing job.get_id() / job.id
    """
    try:
        tc = sutc.TestConfig()
        js = saga.job.Service(tc.js_url, tc.session)

        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        if tc.job_walltime_limit != "":
            jd.wall_time_limit = tc.job_walltime_limit
        if tc.job_project != "":
            jd.project = tc.job_project
        if tc.job_queue != "":
            jd.queue = tc.job_queue

        j = js.create_job(jd)
        j.run()

        assert j.id != None
        assert j.id == j.get_id()

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se

# ------------------------------------------------------------------------------
#
def test_job_wait():
    """ Testing job.wait() - expecting state: DONE (this test might take a while)
    """
    try:
        # we re-use the job from the RUNNING/PENDING state test, to cut waiting
        # time

        assert (long_job != None)
        j1 = long_job
        j1.wait()
        assert j1.state == saga.job.DONE

    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se

