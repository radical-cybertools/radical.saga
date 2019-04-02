
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.saga.exceptions as se

import radical.utils as ru


def test_SagaException():
    try:
        raise se.SagaException('SagaException')
    except    se.SagaException, e:
        assert 'SagaException' in e.get_message(), str(e)
        assert 'SagaException' in str(e)         , str(e)

    try:
        raise se.SagaException('SagaException')
    except    se.NotImplemented:
        assert False
    except Exception, e:
        assert 'SagaException' in e.get_message(), str(e)
        assert 'SagaException' in str(e)         , str(e)

def test_NotImplemented():
    try:
        raise se.NotImplemented('NotImplemented')
    except    se.NotImplemented, e:
        assert 'NotImplemented' in e.get_message(), str(e)
        assert 'NotImplemented' in str(e)         , str(e)

    try:
        raise se.NotImplemented('NotImplemented')
    except    se.Timeout:
        assert False
    except Exception, e:
        assert 'NotImplemented' in e.get_message(), str(e)
        assert 'NotImplemented' in str(e)         , str(e)

def test_IncorrectURL():
    try:
        raise se.IncorrectURL('IncorrectURL')
    except    se.IncorrectURL, e:
        assert 'IncorrectURL' in e.get_message(), str(e)
        assert 'IncorrectURL' in str(e)         , str(e)

    try:
        raise se.IncorrectURL('IncorrectURL')
    except    se.Timeout:
        assert False
    except Exception, e:
        print e
        print str(e)
        assert 'IncorrectURL' in e.get_message(), str(e)
        assert 'IncorrectURL' in str(e)         , str(e)



