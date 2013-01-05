
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" 
List of all registered SAGA adaptor modules.

This registry is used to locate and load adaptors.  The entries must be
formatted in dotted python module notation (e.g. as
'saga.adaptors.context.x509'), and the Python module search path has to be
configured so that the listed modules can be loaded.

Note that a module listed in the registry is not guaranteed to be available:
while the engine will attempt to load the module, it may be disabled by
configure options, or due to missing pre-requisites.  
"""

adaptor_registry = ["saga.adaptors.localjob.localjob",
                    "saga.adaptors.context.myproxy",
                    "saga.adaptors.context.x509",
                    "saga.adaptors.context.userpass",
                    "saga.adaptors.localfile.localfile",
                    "saga.adaptors.localfile.dummyfile",
                    "saga.adaptors.irods.irods_replica"
                   ]


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

