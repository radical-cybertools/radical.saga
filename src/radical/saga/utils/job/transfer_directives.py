
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


''' Provides a parser class for the file transfer specification as it is
    defined in GFD.90, sction 4.1.3.
'''

from ... import exceptions as se


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
# '>'  copies the local file to the remote file before the job starts.
#      Overwrites the remote file if it exists.
# '>>' copies the local file to the remote file before the job starts.
#      Appends to the remote file if it exists.
# '<'  copies the remote file to the local file after the job finishes.
#      Overwrites the local file if it exists.
# '<<' copies the remote file to the local file after the job finishes.
#      Appends to the local file if it exists.


# ------------------------------------------------------------------------------
#
class TransferDirectives(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, directives=None):

        self._in_overwrite  = list()
        self._in_append     = list()
        self._out_overwrite = list()
        self._out_append    = list()

        if not directives:
            directives = []

        for d in directives:

            if (d.count('>') > 2) or (d.count('<') > 2):
                msg = "'%s' is not a valid transfer d string."
                raise se.BadParameter(msg)

            elif '>>' in d:
                (loc, rem) = d.split('>>')
                self._in_append.append([loc.strip(), rem.strip()])

            elif '>' in d:
                (loc, rem) = d.split('>')
                self._in_overwrite.append([loc.strip(), rem.strip()])

            elif '<<' in d:
                (loc, rem) = d.split('<<')
                self._out_append.append([loc.strip(), rem.strip()])

            elif '<' in d:
                (loc, rem) = d.split('<')
                self._out_overwrite.append([loc.strip(), rem.strip()])

            else:
                msg = "'%s' is not a valid transfer directive string." % d
                raise se.BadParameter(msg)


    # --------------------------------------------------------------------------
    #
    def _to_string_list(self):

        slist = list()

        for (loc, rem) in self._in_overwrite:
            slist.append('%s > %s' % (loc, rem))

        for (loc, rem) in self._in_append:
            slist.append('%s >> %s' % (loc, rem))

        for (loc, rem) in self._out_overwrite:
            slist.append('%s < %s' % (loc, rem))

        for (loc, rem) in self._out_append:
            slist.append('%s << %s' % (loc, rem))

        return slist


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str(self._to_string_list())


    # --------------------------------------------------------------------------
    #
    @property
    def in_overwrite(self):

        return self._in_overwrite


    # --------------------------------------------------------------------------
    #
    @property
    def in_append(self):

        return self._in_append


    # --------------------------------------------------------------------------
    #
    @property
    def out_overwrite(self):

        return self._out_overwrite


    # --------------------------------------------------------------------------
    #
    @property
    def out_append(self):

        return self._out_append


    # --------------------------------------------------------------------------
    #
    @property
    def string_list(self):

        return self._to_string_list()


# ------------------------------------------------------------------------------
#
def _test_():

    tdp = TransferDirectives(["ab","a>c", "c>>d","f<a","g<<h"])

    print(tdp.in_append)
    print(tdp.in_overwrite)
    print(tdp.out_append)
    print(tdp.out_overwrite)


# ------------------------------------------------------------------------------

