
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


from ..exceptions import *

# ----------------------------------------------------------------
#
def translate_exception (e, msg=None) :
    """
    In many cases, we should be able to roughly infer the exception cause
    from the error message -- this is centrally done in this method.  If
    possible, it will return a new exception with a more concise error
    message and appropriate exception type.
    """

    if  not issubclass (e.__class__, SagaException) :
        # we do not touch non-saga exceptions
        return e

    if  not issubclass (e.__class__, NoSuccess) :
        # this seems to have a specific cause already, leave it alone
        return e

    import traceback
    import radical.utils as ru
    ru.get_logger('radical.saga.pty').debug (traceback.format_exc())


    cmsg = e._plain_message

    if  msg :
        cmsg = "%s (%s)" % (cmsg, msg)

    lmsg = cmsg.lower ()

    if  'could not resolve hostname' in lmsg :
        e = BadParameter (cmsg)

    elif  'connection timed out' in lmsg :
        e = BadParameter (cmsg)

    elif  'connection refused' in lmsg :
        e = BadParameter (cmsg)

    elif 'auth' in lmsg :
        e = AuthorizationFailed (cmsg)

    elif 'man-in-the-middle' in lmsg :
        e = AuthenticationFailed ("ssh key mismatch detected: %s" % cmsg)

    elif 'pass' in lmsg :
        e = AuthenticationFailed (cmsg)

    elif 'ssh_exchange_identification' in lmsg :
        e = AuthenticationFailed ("too frequent login attempts, or sshd misconfiguration: %s" % cmsg)

    elif 'denied' in lmsg :
        e = PermissionDenied (cmsg)

    elif 'shared connection' in lmsg :
        e = NoSuccess ("Insufficient system resources: %s" % cmsg)

    elif 'pty allocation' in lmsg :
        e = NoSuccess ("Insufficient system resources: %s" % cmsg)

    elif 'Connection to master closed' in lmsg :
        e = NoSuccess ("Connection failed (insufficient system resources?): %s" % cmsg)

    return e

