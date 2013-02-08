
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a convenience class for handling GFD.90-style job ids.
'''

from saga.utils.exception import ExceptionBase

# 4.1.5 Job Identifiers (GFD90 p 177-178)
#
# The JobID is treated as an opaque string in the SAGA API. However, for the 
# sake of interoperability of different SAGA implementations, and for potential 
# extended use of the JobID information, the JobID SHOULD be implemented as:
#
#      '[backend url]-[native id]'
#
# For example, a job submitted to the host remote.host.net via ssh (whose 
# daemon runs on port 22), and having the POSIX PID 1234, should get the job id:
#
#      '[ssh://remote.host.net:22/]-[1234]'

class JobId(object):

    def __init__(self):
        self._backend_url = ''
        self._native_id = ''

    def __str__(self):
        return self.string

    @property
    def string(self):
        doc = "The string property."
        def fget(self):
            return "[%s]-[%s]" % (self._backend_url, self._native_id)
        def fset(self, value):
            if (value[0] != '[') or (value[-1] != ']') or (value.find("]-[") < 0 ):
                raise InvalidJobId(value)
            else:
                (b, n) = value.split("]-[")
                self._backend_url = b[1:]
                self._native_id = n[:-2]
        def fdel(self):
            pass
        return locals()

    @property
    def tuple(self):
        doc = "The tuple property."
        def fget(self):
            return (self._backend_url, self._native_id)
        def fset(self, value):
            (b, n) = value # auto-unpack
            self._backend_url = b
            self._native_id = n
        def fdel(self):
            pass
        return locals()

    @property
    def backend_url(self):
        doc = "The backend-url property."
        def fget(self):
            return self._backend_url
        def fset(self, value):
            self._backend_url = value
        def fdel(self):
            pass
        return locals()

    @property
    def native_id(self):
        doc = "The native-id property."
        def fget(self):
            return self._native_id
        def fset(self, value):
            self._native_id = value
        def fdel(self):
            pass
        return locals()

    @classmethod
    def from_string(self, jobid_string):
        jid = JobId()
        jid.string = jobid_string
        return jid

    @classmethod
    def from_tuple(self, backend_url, native_id):
        jid = JobId()
        jid.tuple = (backend_url, native_id)
        return jid

class InvalidJobId(ExceptionBase):
    def __init__(self, jobid):
        self.message = "'%s' is not a valid job id string." % jobid

def _test_():

    jid = JobId.from_string('[test]-[native]')
    print jid.tuple
    print jid.backend_url
    print jid.native_id
    jid.native_id = 'NNN'
    jid.backend_url = 'BBB'
    print jid


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

