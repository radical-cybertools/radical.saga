
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Global constants
"""

######################################################################
#
# task constansts
#
SYNC      = 1        # 'Sync'
ASYNC     = 2        # 'Async'
TASK      = 3        # 'Task'

UNKNOWN   = 'Unknown'
NEW       = 'New'
RUNNING   = 'Running'
DONE      = 'Done'
FAILED    = 'Failed'
CANCELED  = 'Canceled'
# FINAL     = DONE | FAILED | CANCELED

STATE     = 'State'
RESULT    = 'Result'
EXCEPTION = 'Exception'

ALL       = 'All'
ANY       = 'Any'

######################################################################
# 
# task container constants
#
SIZE   = "Size"
TASKS  = "Tasks"
STATES = "States"


######################################################################
# 
# context container constants
#
TYPE            = "Type"
SERVER          = "Server"
TOKEN           = "Token"
CERT_REPOSITORY = "CertRepository"
USER_PROXY      = "UserProxy"
USER_CERT       = "UserCert"
USER_KEY        = "UserKey"
USER_ID         = "UserID"
USER_PASS       = "UserPass"
USER_VO         = "UserVO"
LIFE_TIME       = "LifeTime"
REMOTE_ID       = "RemoteID"
REMOTE_HOST     = "RemoteHost"
REMOTE_PORT     = "RemotePort"


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

