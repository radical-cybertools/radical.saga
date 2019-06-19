
__author__    = "RADICAL-SAGA Development Team"
__copyright__ = "Copyright 2013, RADICAL"
__license__   = "MIT"


import os
import radical.utils as ru


# ------------------------------------------------------------------------------
#
import utils


# ------------------------------------------------------------------------------
#
from   .constants  import *

from   .task       import Task, Container
from   .attributes import Attributes, Callback
from   .session    import Session, DefaultSession
from   .context    import Context
from   .url        import Url

from   .exceptions import SagaException
from   .exceptions import NotImplemented
from   .exceptions import IncorrectURL
from   .exceptions import BadParameter
from   .exceptions import AlreadyExists
from   .exceptions import DoesNotExist
from   .exceptions import IncorrectState
from   .exceptions import PermissionDenied
from   .exceptions import AuthorizationFailed
from   .exceptions import AuthenticationFailed
from   .exceptions import Timeout
from   .exceptions import NoSuccess

from   .           import job
from   .           import filesystem
from   .           import replica
from   .           import advert
from   .           import resource
# import radical.saga.messages


# ------------------------------------------------------------------------------
#
pwd     = os.path.dirname (__file__)
version_short, version_detail, version_base, version_branch, \
               sdist_name, sdist_path = ru.get_version ([pwd])
version = version_short


# FIXME: the logger init will require a 'classical' ini based config, which is
# different from the json based config we use now.   May need updating once the
# radical configuration system has changed to json
_logger = ru.Logger('radical.saga')
_logger.info ('radical.saga         version: %s' % version_detail)


# ------------------------------------------------------------------------------

