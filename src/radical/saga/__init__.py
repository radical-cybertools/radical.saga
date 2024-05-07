
__author__    = "RADICAL-SAGA Development Team"
__copyright__ = "Copyright 2013, RADICAL"
__license__   = "MIT"


# ------------------------------------------------------------------------------
#
from .constants  import *

from .task       import Task, Container
from .attributes import Attributes, Callback
from .session    import Session, DefaultSession
from .context    import Context
from .url        import Url

from .exceptions import SagaException
from .exceptions import NotImplemented
from .exceptions import IncorrectURL
from .exceptions import BadParameter
from .exceptions import AlreadyExists
from .exceptions import DoesNotExist
from .exceptions import IncorrectState
from .exceptions import PermissionDenied
from .exceptions import AuthorizationFailed
from .exceptions import AuthenticationFailed
from .exceptions import Timeout
from .exceptions import NoSuccess

from .           import job
from .           import filesystem
from .           import replica
from .           import advert
from .           import resource
# from .           import messages

from .           import utils


# ------------------------------------------------------------------------------
#
import os            as _os
import radical.utils as _ru

_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = _ru.get_version(_mod_root)
version      = version_short
__version__  = version_detail


# ------------------------------------------------------------------------------

