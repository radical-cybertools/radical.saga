
import os
import sys
import saga
import saga.utils.test_config as sutc

def test_create_sync_service () :
    """ create a job service instance synchronously """
    try:
        tc = sutc.TestConfig ()
        js = saga.job.Service (tc.js_url, tc.session)
    except saga.SagaException as se:
        print se
        assert False

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

