
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
def test_job_service_get_url(job_service, cfg):
    """ Test if the job service URL is returned correctly
    """
    js = job_service
    try:
        assert js, "job service creation failed?"
        assert (cfg['job_service_url'] == str(js.url))

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex


# ------------------------------------------------------------------------------
#
def test_job_service_invalid_url(cfg, session, tools):
    """ Test if a non-resolvable hostname results in a proper exception
    """

    tmp_js = None
    
    try:
        invalid_url      = saga.Url(cfg['job_service_url'])
        invalid_url.host = "does.not.exist"
        tmp_js = saga.job.Service (invalid_url, session)
        assert False, "Expected XYZ exception but got none."

    except saga.BadParameter :
        assert True

    # we don't check DNS anymore, as that can take *ages* -- so we now also
    # see Timeout and NoSuccess exceptions...
    except (saga.NoSuccess, saga.Timeout) :
        assert True

    # other exceptions should never occur
    except saga.SagaException as ex:
        assert False, "Expected BadParameter, Timeout or NoSuccess exception, but got %s (%s)" % (type(ex), ex)

    finally :
        tools.silent_close (tmp_js)


# ------------------------------------------------------------------------------
#
def test_job_service_create(job_service, cfg, tools):
    """ Test service.create_job() - expecting state 'NEW'
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)
        assert j.state == j.get_state()
        assert j.state == saga.job.NEW

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally :
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_job_service_get_session():
    """ Test if the job service session is set correctly
    """
    js = None
    session = None
    try:
        tc = testing.get_test_config ()
        session = tc.session or saga.Session()
        js = saga.job.Service(tc.job_service_url, session)

        assert js.get_session() == session, "Setting service session failed."
        assert js.session == session, "Setting service session failed."
        assert js._adaptor.get_session() == session, "Setting service session failed."
        assert js._adaptor.session == session, "Setting service session failed."

    except saga.SagaException as ex:
        assert False, "unexpected exception %s" % ex
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_job_run (job_service, cfg, tools):
    """ Test job.run() - expecting state: RUNNING/PENDING
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)

        j.run()

        assert (j.state in [saga.job.RUNNING, saga.job.PENDING]), "j.state: %s" % j.state

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally :
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_job_wait(job_service, cfg, tools):
    """ Test job.wait() - expecting state: DONE (this test might take a while)
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['10']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)

        j.run()
        j.wait()
        assert j.state == saga.job.DONE, "%s != %s" % (j.state, saga.job.DONE)

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally :
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_job_multiline_run(job_service, cfg, tools):
    """ Test job.run() with multiline command
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sh'
        jd.arguments  = ["""-c "python -c '
import time
if True :
  if True :
    time.sleep (3)
'
"
"""]

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)
        j.run()

        assert (j.state in [saga.job.RUNNING, saga.job.PENDING])
        j.wait()
        assert (j.state in [saga.job.DONE])

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally :
        tools.silent_cancel (j)



# ------------------------------------------------------------------------------
#
def test_job_suspend_resume(job_service, cfg, tools):
    """ Test job.suspend()/resume() - expecting state: SUSPENDED/RUNNING
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['20']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)
        j.run()
        j.suspend()

        assert j.state == saga.job.SUSPENDED
        assert j.state == j.get_state()

        j.resume()
        assert j.state == saga.job.RUNNING
        assert j.state == j.get_state()

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally:
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_job_cancel(job_service, cfg, tools):
    """ Test job.cancel() - expecting state: CANCELED
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['10']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)

        j.run()
        j.cancel()
        assert j.state == saga.job.CANCELED

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally:
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_job_run_many(job_service, cfg, tools):
    """ Run a bunch of jobs concurrently via the same job service.
    """
    NUM_JOBS = 32

    js   = job_service
    jobs = []
    try:

        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments  = ['60']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        for i in range(0, NUM_JOBS):
            j = js.create_job(jd)
            jobs.append(j)

        # start all jobs
        for job in jobs:
            job.run()

        for job in jobs:
            job.cancel()
            assert job.state == saga.job.CANCELED

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally:
        tools.silent_cancel (jobs)


# ------------------------------------------------------------------------------
#
def test_get_exit_code(job_service, cfg, tools):
    """ Test job.exit_code
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = "/bin/sleep"

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)
        j.run()
        j.wait()

        ec = j.exit_code
        assert ec == 1, "%s != 1" % ec

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally:
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_get_stdio():
    """ Test job.get_stdin/get_stdout/get_log
    """
    js = None
    j  = None
    try:
        tc = testing.get_test_config ()
        js = saga.job.Service(tc.job_service_url, tc.session)
        jd = saga.job.Description()
        jd.pre_exec   = ['echo pre' ]
        jd.executable = 'sh'
        jd.arguments  = ['-c', '"echo out; echo err 1>&2"']
        jd.post_exec  = ['echo post']

        # add options from the test .cfg file if set
        jd = sutc.add_tc_params_to_jd(tc=tc, jd=jd)

        j = js.create_job(jd)
        j.run()
        j.wait()

        assert 0      == j.exit_code

        assert 'pre'  in j.get_log()
        assert 'post' in j.get_log()
        assert 'out'  in j.get_stdout()
        assert 'err'  in j.get_stderr()

        assert 'pre'  in j.log
        assert 'post' in j.log
        assert 'out'  in j.stdout
        assert 'err'  in j.stderr

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
def test_get_service_url(job_service,cfg, tools):
    """ Test if job.service_url == Service.url
    """
    js = job_service
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)

        assert j.service_url == js.url

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally:
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------
#
def test_get_id(job_service, cfg, tools):
    """ Test job.get_id() / job.id
    """
    js = job_service
    j  = None
    try:
        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        tools.configure_jd (cfg, jd)

        j = js.create_job(jd)
        j.run()

        assert j.id is not None
        assert j.id == j.get_id()

    except saga.SagaException as e:
        tools.assert_exception (cfg, e)

    finally:
        tools.silent_cancel (j)


# ------------------------------------------------------------------------------

