
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from   saga.constants  import *

import saga


class Context (saga.base.Base, saga.Attributes) :
    '''A SAGA Context object as defined in GFD.90.

    A security context is a description of a security token.  It is important to
    understand that, in general, a context really just *describes* a token, but
    that a context *is not* a token (*). For example, a context may point to
    a X509 certificate -- but it will in general not hold the certificate
    contents.

    Context classes are used to inform the backends used by Bliss on what
    security tokens are expected to be used.  By default, Bliss will be able to
    pick up such tokens from their default location, but in some cases it might
    be necessary to explicitly point to them - then use a L{Session} with
    context instances to do so.

    The usage example for contexts is below::

        # define an ssh context
        c = saga.Context()
        c.context_type = 'ssh'
        c.user_cert = '$HOME/.ssh/special_id_rsa'
        c.user_key = '$HOME/.ssh/special_id_rsa.pub'

        # add the context to a session
        s = saga.Session()
        s.contexts.append(c)

        # create a job service in this session -- that job service can now
        # *only* use that ssh context. 
        j = saga.job.Service('ssh://remote.host.net/', s)


    The L{Session} argument to the L{job.Service} constructor is fully optional
    -- if left out, Bliss will use default session, which picks up some default
    contexts as described above -- that will suffice for the majority of use
    cases.

    ----

    (*) The only exception to this rule is the 'UserPass' key, which is used to
    hold plain-text passwords.  Use this key with care -- it is not good
    practice to hard-code passwords in the code base, or in config files.
    Also, be aware that the password may show up in log files, when debugging or
    analyzing your application.

    '''

    def __init__ (self, type, _adaptor=None, _adaptor_state={}) : 
        '''
        type: string
        ret:  None
        '''

        import saga.attributes as sa

        saga.base.Base.__init__ (self, type.lower(), _adaptor, _adaptor_state, type, ttype=None)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register  (TYPE,            None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (SERVER,          None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CERT_REPOSITORY, None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_PROXY,      None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_CERT,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_KEY,        None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_ID,         None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_PASS,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_VO,         None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (LIFE_TIME,       -1,   sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (REMOTE_ID,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (REMOTE_HOST,     None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (REMOTE_PORT,     None, sa.STRING, sa.VECTOR, sa.WRITEABLE)

        self.type = type


    def _initialize (self, session) :
        '''
        ret:  None
        '''
        return self._adaptor._initialize (session)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

