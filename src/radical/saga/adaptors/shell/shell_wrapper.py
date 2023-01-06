
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os

# --------------------------------------------------------------------
# server side job management script
with open(os.path.dirname(__file__) + '/shell_wrapper.sh') as fh:
    _WRAPPER_SCRIPT = fh.read ()
