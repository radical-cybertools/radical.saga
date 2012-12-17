# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, Ole Christian Weidner"
__license__   = "MIT"

_adaptor_registry = []

_adaptor_registry.append({"module"   : "saga.adaptors.local.localjob",  "class" : "LocalJobAdaptor"})
_adaptor_registry.append({"module"   : "saga.adaptors.ssh.job",         "class" : "SSHJobAdaptor"})
_adaptor_registry.append({"module"   : "saga.adaptors.sftp.sftpfile",   "class" : "SFTPFilesystemAdaptor"})
_adaptor_registry.append({"module"   : "saga.adaptors.sge.sgesshjob",   "class" : "SGEJobAdaptor"})
_adaptor_registry.append({"module"   : "saga.adaptors.pbs.pbsshjob",    "class" : "PBSJobAdaptor"})

