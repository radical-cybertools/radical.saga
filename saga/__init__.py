
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


# ------------------------------------------------------------------------------
#
from   saga.constants      import *

from   saga.task           import Task, Container
from   saga.attributes     import Attributes, Callback
from   saga.session        import Session
from   saga.context        import Context
from   saga.url            import Url

from   saga.exceptions     import SagaException
from   saga.exceptions     import NotImplemented
from   saga.exceptions     import IncorrectURL
from   saga.exceptions     import BadParameter
from   saga.exceptions     import AlreadyExists
from   saga.exceptions     import DoesNotExist
from   saga.exceptions     import IncorrectState
from   saga.exceptions     import PermissionDenied
from   saga.exceptions     import AuthorizationFailed
from   saga.exceptions     import AuthenticationFailed
from   saga.exceptions     import Timeout
from   saga.exceptions     import NoSuccess

import saga.job
import saga.filesystem
import saga.replica
import saga.advert
import saga.resource


# ------------------------------------------------------------------------------
#
import os
import radical.utils.logger as rul

version=open    (os.path.dirname (os.path.abspath (__file__)) + "/VERSION", 'r').read().strip()
# rul.log_version ('saga', 'saga-python', version)
rul.getLogger ('saga').info ('saga-python     version: %s' % version)


# ------------------------------------------------------------------------------

