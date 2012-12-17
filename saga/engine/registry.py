# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, Ole Christian Weidner"
__license__   = "MIT"

adaptor_registry = ["saga.adaptors.local.localjob",
                    "saga.adaptors.ssh.job"       ,
                    "saga.adaptors.sftp.sftpfile" ,
                    "saga.adaptors.sge.sgesshjob" ,
                    "saga.adaptors.pbs.pbsshjob"  ]

