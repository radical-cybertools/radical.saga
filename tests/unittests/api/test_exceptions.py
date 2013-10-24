
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.exceptions as se

import radical.utils as ru


def test_SagaException():
    try:
        raise se.SagaException('SagaException')
    except    se.SagaException, e:
        assert e.get_message() == 'SagaException'
        assert str(e)          == 'SagaException'

    try:
        raise se.SagaException('SagaException')
    except    se.NotImplemented:
        assert False
    except Exception, e:
        assert e.get_message() == 'SagaException'
        assert str(e)          == 'SagaException'

def test_NotImplemented():
    try:
        raise se.NotImplemented('NotImplemented')
    except    se.NotImplemented, e:
        assert e.get_message() == 'NotImplemented'
        assert str(e)          == 'NotImplemented'

    try:
        raise se.NotImplemented('NotImplemented')
    except    se.Timeout:
        assert False
    except Exception, e:
        assert e.get_message() == 'NotImplemented'
        assert str(e)          == 'NotImplemented'

def test_IncorrectURL():
    try:
        raise se.IncorrectURL('IncorrectURL')
    except    se.IncorrectURL, e:
        assert e.get_message() == 'IncorrectURL'
        assert str(e)          == 'IncorrectURL'

    try:
        raise se.IncorrectURL('IncorrectURL')
    except    se.Timeout:
        assert False
    except Exception, e:
        assert e.get_message() == 'IncorrectURL'
        assert str(e)          == 'IncorrectURL'



