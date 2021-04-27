#!/usr/bin/env python

# pylint: disable=unused-argument,no-member

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import os

import radical.saga                   as rs
import radical.utils                  as ru
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
    except Exception:
        pass


# ------------------------------------------------------------------------------
#
def test_close():
    """ Test job service close()
    """
    cfg = config()

    try:
        js = rs.job.Service(cfg.job_service_url, cfg.session)
        js.close()
        js.get_url()
        assert False, "Subsequent calls should fail after close()"

    except rs.NotImplemented as ni:
        assert cfg.notimpl_warn_only, "%s " % ni
        if cfg.notimpl_warn_only:
            print("%s " % ni)
    except rs.SagaException:
        assert True


# ------------------------------------------------------------------------------
#
def test_open_close():
    """ Test job service create / close() in a big loop
    """
    js  = None
    cfg = config()

    try:
        for _ in range(0, 10):
            js = rs.job.Service(cfg.job_service_url, cfg.session)
            js.close()

    except rs.NotImplemented as ni:
        assert cfg.notimpl_warn_only, "%s " % ni
        if cfg.notimpl_warn_only:
            print("%s " % ni)
    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_get_url():
    """ Test job service url/get_url()
    """
    js  = None
    cfg = config()

    try:
        js = rs.job.Service(cfg.job_service_url, cfg.session)
        assert(str(js.get_url()) == str(cfg.job_service_url)), \
                'expected %s [%s]' % (js.get_url(), cfg.job_service_url)
        assert(str(js.url)       == str(cfg.job_service_url)), \
                'expected %s [%s]' % (js.get_url(), cfg.job_service_url)

    except rs.NotImplemented as ni:
        assert cfg.notimpl_warn_only, "%s " % ni
        if cfg.notimpl_warn_only:
            print("%s " % ni)
    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_list_jobs():
    """ Test if a submitted job shows up in Service.list() """
    j   = None
    js  = None
    cfg = config()

    try:
        js = rs.job.Service(cfg.job_service_url, cfg.session)

        # create job service and job
        jd = rs.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.configure_jd(cfg=cfg, jd=jd)

        j = js.create_job(jd)

        # run job - now it has an id, and js must know it
        j.run()
        all_jobs = js.list()
        assert j.id in all_jobs, \
            "%s not in %s" % (j.id, all_jobs)

    except rs.NotImplemented as ni:
        assert cfg.notimpl_warn_only, "%s " % ni
        if cfg.notimpl_warn_only:
            print("%s " % ni)
    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_run_job():
    """ Test to submit a job via run_job, and retrieve id"""
    js  = None
    cfg = config()

    try:
        js = rs.job.Service(cfg.job_service_url, cfg.session)

        # create job service and job
        j = js.run_job("/bin/sleep 10")
        assert j.id

    except rs.NotImplemented as ni:
        assert cfg.notimpl_warn_only, "%s " % ni
        if cfg.notimpl_warn_only:
            print("%s " % ni)
    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def test_get_job():
    """ Test to submit a job, and retrieve it by id """
    j   = None
    js  = None
    cfg = config()

    try:
        js = rs.job.Service(cfg.job_service_url, cfg.session)

        # create job service and job
        jd = rs.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['10']

        # add options from the test .cfg file if set
        jd = sutc.configure_jd(cfg=cfg, jd=jd)

        j = js.create_job(jd)

        # run job - now it has an id, and js must be able to retrieve it by id
        j.run()
        j_clone = js.get_job(j.id)
        assert j.id in j_clone.id

    except rs.NotImplemented as ni:
        assert cfg.notimpl_warn_only, "%s " % ni
        if cfg.notimpl_warn_only:
            print("%s " % ni)
    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    finally:
        _silent_cancel(j)
        _silent_close_js(js)


# ------------------------------------------------------------------------------
#
def helper_multiple_services(i):

    cfg = config()
    js1 = rs.job.Service(cfg.job_service_url, cfg.session)
    js2 = rs.job.Service(cfg.job_service_url, cfg.session)
    jd  = rs.job.Description()

    jd.executable = '/bin/sleep'
    jd.arguments  = ['3']
    jd = sutc.configure_jd(cfg=cfg, jd=jd)

    j1 = js1.create_job(jd)
    j2 = js2.create_job(jd)
    j1.run()
    j2.run()

    assert (j1.state in [rs.job.RUNNING, rs.job.PENDING])
    assert (j2.state in [rs.job.RUNNING, rs.job.PENDING])

    j1.wait()
    j2.wait()

    assert (j1.state == rs.job.DONE)
    assert (j2.state == rs.job.DONE)

    _silent_close_js(js1)
    _silent_close_js(js2)


# ------------------------------------------------------------------------------
#
NUM_SERVICES = 5


def test_multiple_services():
    """
    test concurrent job services (takes a while)
    """
    import threading as mt
    import queue

    q = queue.Queue()

    def _test_ms(i):
        cfg = config()

        try:
            helper_multiple_services(i)
            q.put(True)

        except rs.NotImplemented as ni:
            assert cfg.notimpl_warn_only, "%s " % ni
            if cfg.notimpl_warn_only:
                print("%s " % ni)
            q.put(True)

        except rs.SagaException as se:
            q.put(False)
            assert False, "Unexpected exception: %s" % se

        except:
            q.put(False)
            assert False, "Unexpected exception"

    threads = list()
    for i in range(0, NUM_SERVICES):
        t = mt.Thread(target=_test_ms, args=[i])
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    for t in threads:
        ret = q.get_nowait()
        assert(ret is True), 'test failed'


# ------------------------------------------------------------------------------
#
def test_jobid_viability ():
    """ Test if jobid represents job """
    # The job id for the fork shell adaptor should return a pid which represents
    # the actual job instance.  We test by killing that pid and checking state.

    try:
        cfg = config()

        js_url = rs.Url (cfg.job_service_url)
        if  js_url.schema.lower() not in ['fork', 'local', 'ssh'] :
            # test not supported for other backends
            return

        if  js_url.host.lower() not in [None, '', 'localhost'] :
            # test not supported for other backends
            return

        js  = rs.job.Service ('fork:///')
        j   = js.run_job ("/bin/sleep 100")
        jid = j.id

        _, j_part = jid.split('-', 1)
        pid = j_part[1:-3]

        # kill the children (i.e. the only child) of the pid, which is the
        # actual job
        os.system('pkill -TERM -P %s' % pid)

        assert (j.state == rs.job.FAILED), 'job.state: %s' % j.state

        _silent_close_js(js)

    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se



# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_close()
    test_open_close()
    test_get_url()
    test_list_jobs()
    test_run_job()
    test_get_job()
    test_multiple_services()
    test_jobid_viability()


# ------------------------------------------------------------------------------

