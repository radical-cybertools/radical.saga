# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" List of all registered SAGA adaptors. 
"""

adaptor_registry = ["saga.adaptors.localjob.localjob",
                    "saga.adaptors.context.myproxy",
                    "saga.adaptors.context.x509",
                    "saga.adaptors.context.userpass",
                    "saga.adaptors.saga_adaptor_filesystem_local",
                    "saga.adaptors.saga_adaptor_filesystem_dummy",
                    "saga.adaptors.saga_adaptor_replica_irods"
                   ]

