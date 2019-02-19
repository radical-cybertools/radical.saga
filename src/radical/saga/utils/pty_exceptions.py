
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"

import traceback
import radical.utils as ru

from ..import exceptions as se


# ------------------------------------------------------------------------------
#
def translate_exception (e, msg=None) :
    """
    In many cases, we should be able to roughly infer the exception cause
    from the error message -- this is centrally done in this method.  If
    possible, it will return a new exception with a more concise error
    message and appropriate exception type.
    """

    if  not issubclass (e.__class__, se.SagaException) :
        # we do not touch non-saga exceptions
        return e

    if  not issubclass (e.__class__, se.NoSuccess) :
        # this seems to have a specific cause already, leave it alone
        return e

    ru.Logger('radical.saga.pty').debug (traceback.format_exc())


    cmsg = e._plain_message

    if  msg :
        cmsg = "%s (%s)" % (cmsg, msg)

    lmsg = cmsg.lower ()

    if   'could not resolve hostname' in lmsg: e = se.BadParameter (cmsg)
    elif 'connection timed out'       in lmsg: e = se.BadParameter (cmsg)
    elif 'connection refused'         in lmsg: e = se.BadParameter (cmsg)
    elif 'auth'                       in lmsg: e = se.AuthorizationFailed(cmsg)
    elif 'pass'                       in lmsg: e = se.AuthenticationFailed(cmsg)
    elif 'denied'                     in lmsg: e = se.PermissionDenied (cmsg)
    elif 'man-in-the-middle'          in lmsg: 
        e = se.AuthenticationFailed("ssh key mismatch: %s" % cmsg)
    elif 'ssh_exchange_identification' in lmsg :
        e = se.AuthenticationFailed ("too many login attempts: %s" % cmsg)
    elif 'shared connection' in lmsg :
        e = se.NoSuccess ("Insufficient system resources: %s" % cmsg)
    elif 'pty allocation' in lmsg :
        e = se.NoSuccess ("Insufficient system resources: %s" % cmsg)
    elif 'Connection to master closed' in lmsg :
        e = se.NoSuccess ("Connection closed by system: %s" % cmsg)

    return e


# ------------------------------------------------------------------------------

