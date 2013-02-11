
import os
import sys
import saga
import saga.utils.test_config as sutc

# ------------------------------------------------------------------------------
#
def test_create_sync_service () :
    """ create a job service instance synchronously """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
    except saga.SagaException as se:
        assert False

# ------------------------------------------------------------------------------
#
def test_get_url () :
    """ create a job service and check url """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
        assert str(js.get_url ()) == str(tc.js_url)

    except saga.SagaException as se:
        assert False

# ------------------------------------------------------------------------------
#
def test_create_simple_job () :
    """ create a simple job (/bin/true) """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
        jd = saga.job.Description ()
        jd.executable = '/bin/true'
        j  = js.create_job (jd)
        assert j.state == saga.job.NEW

    except saga.SagaException as se:
        assert False

# ------------------------------------------------------------------------------
#
def test_list_jobs () :
    """ find a submitted job in the list returned by list() """
    try:
        tc = sutc.TestConfig ()

        # create job service and job
        js = saga.job.Service (tc.js_url, tc.session)
        jd = saga.job.Description ()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['10']
        j  = js.create_job (jd)

        # run job - now it has an id, and js must know it
        j.run ()
        assert j.id in js.list ()

    except saga.SagaException as se:
        print se
        assert False

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

    except saga.SagaException as se:
        print se
        assert False

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
        j  = js.create_job (jd)

        # run job - now it has an id, and js must be able to retrieve it by id
        j.run ()
        j_clone = js.get_job (j.id)
        assert j.id in j_clone.id

    except saga.SagaException as se:
        print se
        assert False

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

