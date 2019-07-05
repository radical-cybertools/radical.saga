#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import time
import unittest

import radical.saga  as rs
import radical.utils as ru

import radical.saga.utils.test_config as sutc


# ------------------------------------------------------------------------------
#
def config():

    ru.set_test_config(ns='radical.saga')
    ru.add_test_config(ns='radical.saga', cfg_name='fork_localhost')

    return ru.get_test_config()


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
class TestJob(unittest.TestCase):

    # --------------------------------------------------------------------------
    #
    def  setUp(self):

        self.cfg     = config()
        self.session = rs.Session()
        self.js      = rs.job.Service(self.cfg.job_service_url, self.session)

        assert self.js.get_session()          == self.session
        assert self.js.session                == self.session
        assert self.js._adaptor.get_session() == self.session
        assert self.js._adaptor.session       == self.session


    # --------------------------------------------------------------------------
    #
    def shutDown(self):

        _silent_close_js(self.js)


    # --------------------------------------------------------------------------
    #
    def test_job_service_get_url(self):
        """ Test if the job service URL is returned correctly
        """
        try:
            assert self.js, "job service creation failed?"
            assert (self.cfg['job_service_url'] == str(self.js.url))

        except rs.SagaException as ex:
            assert False, "unexpected exception %s" % ex


    # --------------------------------------------------------------------------
    #
    def test_job_service_invalid_url(self):
        """ Test if a non-resolvable hostname results in a proper exception
        """

        tmp_js = None

        try:
            invalid_url      = rs.Url(self.cfg['job_service_url'])
            invalid_url.host = "does.not.exist"
            tmp_js = rs.job.Service (invalid_url, self.session)
            assert False, "Expected XYZ exception but got none."

        except rs.BadParameter :
            assert True

        # we don't check DNS anymore, as that can take *ages* -- so we now also
        # see Timeout and NoSuccess exceptions...
        except (rs.NoSuccess, rs.Timeout) :
            assert True

        # other exceptions should never occur
        except rs.SagaException as ex:
            assert False, "Expected BadParameter, Timeout or NoSuccess exception, but got %s (%s)" % (type(ex), ex)

        finally :
            _silent_close_js(tmp_js)


    # --------------------------------------------------------------------------
    #
    def test_job_service_create(self):
        """ Test service.create_job() - expecting state 'NEW'
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments = ['10']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)
            assert j.state == j.get_state()
            assert j.state == rs.job.NEW

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally :
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_job_run (self):
        """ Test job.run() - expecting state: RUNNING/PENDING
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments = ['10']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)

            j.run()

            assert (j.state in [rs.job.RUNNING, rs.job.PENDING]), "j.state: %s" % j.state

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally :
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_job_wait(self):
        """ Test job.wait() - expecting state: DONE (this test might take a while)
        """
        j  = None
        try:
            t_min = time.time()
            time.sleep(0.5)

            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments  = ['10']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)

            j.run()
            j.wait()

            time.sleep(0.5)
            t_max = time.time()

            # assert success
            assert(j.state == rs.job.DONE), "%s != %s" % (j.state, rs.job.DONE)

            # expect job time information is be reported in seconds since epoch
            assert(t_min <= j.created  <= t_max)
            assert(t_min <= j.started  <= t_max)
            assert(t_min <= j.finished <= t_max)

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally :
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_job_multiline_run(self):
        """ Test job.run() with multiline command
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sh'
            jd.arguments  = ["""-c "python -c '
import time
if True :
  if True :
    time.sleep (3)
' " """]

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)
            j.run()

            assert (j.state in [rs.job.RUNNING, rs.job.PENDING])
            j.wait()
            assert (j.state in [rs.job.DONE])

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally :
            sutc.silent_cancel (j)



    # --------------------------------------------------------------------------
    #
    def test_job_suspend_resume(self):
        """ Test job.suspend()/resume() - expecting state: SUSPENDED/RUNNING
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments  = ['20']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)
            j.run()
            j.suspend()

            assert j.state == rs.job.SUSPENDED
            assert j.state == j.get_state()

            j.resume()
            assert j.state == rs.job.RUNNING
            assert j.state == j.get_state()

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally:
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_job_cancel(self):
        """ Test job.cancel() - expecting state: CANCELED
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments  = ['10']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)

            j.run()
            j.cancel()
            assert j.state == rs.job.CANCELED

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally:
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_job_run_many(self):
        """ Run a bunch of jobs concurrently via the same job service.
        """
        NUM_JOBS = 32

        jobs = []
        try:

            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments  = ['60']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            for i in range(0, NUM_JOBS):
                j = self.js.create_job(jd)
                jobs.append(j)

            # start all jobs
            for job in jobs:
                job.run()

            for job in jobs:
                job.cancel()
                assert job.state == rs.job.CANCELED

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally:
            sutc.silent_cancel (jobs)


    # --------------------------------------------------------------------------
    #
    def test_get_exit_code(self):
        """ Test job.exit_code
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = "/bin/sleep"

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)
            j.run()
            j.wait()

            ec = j.exit_code
            assert ec == 1, "%s != 1" % ec

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally:
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_get_stdio(self):
        """ Test job.get_stdin/get_stdout/get_log
        """
        j  = None
        try:
            cfg = config()
            jd = rs.job.Description()

            jd.pre_exec   = ['echo pre' ]
            jd.executable = 'sh'
            jd.arguments  = ['-c', '"echo out; echo err 1>&2"']
            jd.post_exec  = ['echo post']

            # add options from the test .cfg file if set
            jd = sutc.configure_jd(cfg=cfg, jd=jd)
            j  = self.js.create_job(jd)
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

        except rs.NotImplemented as ni:
            assert cfg.notimpl_warn_only, "%s " % ni
            if cfg.notimpl_warn_only:
                print "%s " % ni
        except rs.SagaException as se:
            assert False, "Unexpected exception: %s" % se
        finally:
            _silent_cancel(j)


    # --------------------------------------------------------------------------
    #
    def test_get_service_url(self):
        """ Test if job.service_url == Service.url
        """
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments = ['10']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)

            assert j.service_url == self.js.url

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally:
            sutc.silent_cancel (j)


    # --------------------------------------------------------------------------
    #
    def test_get_id(self):
        """ Test job.get_id() / job.id
        """
        j  = None
        try:
            jd = rs.job.Description()
            jd.executable = '/bin/sleep'
            jd.arguments = ['10']

            # add options from the test .cfg file if set
            sutc.configure_jd (self.cfg, jd)

            j = self.js.create_job(jd)
            j.run()

            assert j.id is not None
            assert j.id == j.get_id()

        except rs.SagaException as e:
            sutc.assert_exception (self.cfg, e)

        finally:
            sutc.silent_cancel (j)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tj = TestJob()
  # tj.test_job_service_get_url()
  # tj.test_job_service_invalid_url()
  # tj.test_job_service_create()
  # tj.test_job_service_get_session()
  # tj.test_job_run()
    tj.test_job_wait()
  # tj.test_job_multiline_run()
  # tj.test_job_suspend_resume()
  # tj.test_job_cancel()
  # tj.test_job_run_many()
  # tj.test_get_exit_code()
  # tj.test_get_stdio()
  # tj.test_get_service_url()
  # tj.test_get_id()


# ------------------------------------------------------------------------------

