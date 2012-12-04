# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a parser class for the file transfer specification as it is
    defined in GFD.90, sction 4.1.3.
'''

from saga.utils.exception import BaseException

# 4.1.3 File Transfer Specifications (GFD90 p 176-177)
#
# The syntax of a file transfer directive for the job description is modeled on 
# the LSF syntax (LSF stands for Load Sharing Facility, a commercial job 
# scheduler by Platform Computing), and has the general syntax:
# local_file operator remote_file
# Both the local_file and the remote_file can be URLs. If they are not URLs,
# but full or relative pathnames, then the local_file is relative to the host 
# where the submission is executed, and the remote_file is evaluated on the 
# execution host of the job. The operator is one of the following four:
#
# ’>’  copies the local file to the remote file before the job starts. 
#      Overwrites the remote file if it exists.
# ’>>’ copies the local file to the remote file before the job starts. 
#      Appends to the remote file if it exists.
# ’<’  copies the remote file to the local file after the job finishes. 
#      Overwrites the local file if it exists.
# ’<<’ copies the remote file to the local file after the job finishes. 
#      Appends to the local file if it exists.

class TransferDirectives(object):

    def __init__(self, directives_list):
        self._in_overwrite = dict()
        self._in_append = dict()
        self._out_overwrite = dict()
        self._out_append = dict()

        # each line in directives_list should contain one directive
        for directive in directives_list:
            if (directive.count('>') > 2) or (directive.count('<') > 2):
                raise InvalidTransferDirective(directive)
            elif '<<' in directive:
                (remote, local) = directive.split('<<')
                self._out_append[remote] = local
            elif '>>' in directive:
                (local, remote) = directive.split('>>')
                self._in_append[local] = remote
            elif '<' in directive:
                (remote, local) = directive.split('<')
                self._out_overwrite[remote] = local
            elif '>' in directive:
                (local, remote) = directive.split('>')
                self._in_overwrite[local] = remote
            else:
                raise InvalidTransferDirective(directive)

    @property
    def in_overwrite(self):
        return self._in_overwrite

    @property
    def in_append(self):
        return self._in_append

    @property
    def out_overwrite(self):
        return self._out_overwrite

    @property
    def out_append(self):
        return self._out_append

class InvalidTransferDirective(BaseException):
    def __init__(self, directive):
        self.message = "'%s' is not a valid transfer directive string." % directive

    def __str__(self):
        return self.message  

def _test_():
    tdp = TransferDirectives(["ab","a>c", "c>>d","f<a","g<<h"])
    print tdp.in_append
    print tdp.in_overwrite
    print tdp.out_append
    print tdp.out_overwrite


