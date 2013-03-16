
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from saga.exceptions import *

def test_SagaException():
    try:
        raise SagaException('SagaException')
    except SagaException, e:
        assert e.get_message() == 'SagaException'
        assert str(e) == 'SagaException'

    try:
        raise SagaException('SagaException')
    except NotImplemented:
        assert False
    except Exception, e:
        assert e.get_message() == 'SagaException'
        assert str(e) == 'SagaException'

def test_NotImplemented():
    try:
        raise NotImplemented('NotImplemented')
    except NotImplemented, e:
        assert e.get_message() == 'NotImplemented'
        assert str(e) == 'NotImplemented'

    try:
        raise NotImplemented('NotImplemented')
    except Timeout:
        assert False
    except Exception, e:
        assert e.get_message() == 'NotImplemented'
        assert str(e) == 'NotImplemented'

def test_IncorrectURL():
    try:
        raise IncorrectURL('IncorrectURL')
    except IncorrectURL, e:
        assert e.get_message() == 'IncorrectURL'
        assert str(e) == 'IncorrectURL'

    try:
        raise IncorrectURL('IncorrectURL')
    except Timeout:
        assert False
    except Exception, e:
        assert e.get_message() == 'IncorrectURL'
        assert str(e) == 'IncorrectURL'

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

