# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a parser class for the file transfer specification as it is
    defined in GFD.90, sction 4.1.3.
'''
# 4.1.3 File Transfer Specifications
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

class TransferDirectiveParser(object):
    def __init__(self, directives_list):
        # each line in directives_list should contain one directive
        for directive in directives_list:
            print directive


def _test_():
    tdp = TransferDirectiveParser(["a>b","c>>d","f<a","g<<h"])