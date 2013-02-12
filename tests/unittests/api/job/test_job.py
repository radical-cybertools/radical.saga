
import os
import sys
import saga
import saga.utils.test_config as sutc

# ------------------------------------------------------------------------------
#
def test_job_states () :
    """ test different job states """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
        jd = saga.job.Description ()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['3']

        j1 = js.create_job (jd)
        assert j1.state == j1.get_state ()
        assert j1.state == saga.job.NEW
                         
        j1.run ()        
        assert j1.state == saga.job.RUNNING
        assert j1.state == j1.get_state ()
                         
        j1.suspend ()    
        assert j1.state == saga.job.SUSPENDED
        assert j1.state == j1.get_state ()
                         
        j1.resume ()     
        assert j1.state == saga.job.RUNNING
        assert j1.state == j1.get_state ()
                         
        j1.wait ()       
        assert j1.state == saga.job.DONE
        assert j1.state == j1.get_state ()


        j2 = js.run_job ("/bin/sleep 10")
        assert j2.state == saga.job.RUNNING
                         
        j2.cancel ()     
        assert j2.state == saga.job.CANCELED


        j3 = js.run_job ("/bin/true")
        assert j3.state == saga.job.RUNNING
                         
        j3.wait ()       
        assert j3.state == saga.job.DONE


        j4 = js.run_job ("/bin/false")
        assert j4.state == saga.job.RUNNING
                         
        j4.wait ()       
        assert j4.state == saga.job.FAILED


    except saga.SagaException as se:
        print  se
        assert False


# ------------------------------------------------------------------------------
#
def test_get_exit_code () :
    """ run a job service and get exit code """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
        j  = js.run_job ("/bin/sh -c 'exit 3'")
        j.wait ()
        assert j.exit_code == 3

    except saga.SagaException as se:
        print  se
        assert False


# ------------------------------------------------------------------------------
#
def test_get_service_url () :
    """ run a job and check service url """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
        j  = js.run_job ("/bin/sleep 10")
        print      j.service_url 
        print type(j.service_url)

        print      js.url
        print type(js.url)

        assert j.service_url == js.url
        assert j.id == j.get_id ()

    except saga.SagaException as se:
        print  se
        assert False


# ------------------------------------------------------------------------------
#
def test_get_id () :
    """ run a job service and get id """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
        j  = js.run_job ("/bin/sleep 10")
        assert j.id != None
        assert j.id == j.get_id ()

    except saga.SagaException as se:
        print  se
        assert False


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

