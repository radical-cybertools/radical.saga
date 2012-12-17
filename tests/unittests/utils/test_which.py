# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

from saga.utils.which import *

def test_which():
    assert which('doesnotexistatall') is None
    if os.path.isfile('/usr/bin/date'):
        assert which('date') == '/usr/bin/date'
    if os.path.isfile('/bin/date'):
        assert which('date') == '/bin/date'

