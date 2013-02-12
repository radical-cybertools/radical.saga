
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import saga.utils.singleton
import saga.utils.logger
import saga.engine.engine
import saga.base


class _DefaultSession (object) :

    __metaclass__ = saga.utils.singleton.Singleton

    def __init__ (self) :

        # the default session picks up default contexts, from all context
        # adaptors.  To implemented, we have to do some legwork: get the engine,
        # dig through the registered context adaptors, and ask each of them for
        # default contexts.

        self._contexts = []
        self._engine   = saga.engine.engine.Engine ()
        self._logger   = saga.utils.logger.getLogger ('saga.DefaultSession')

        if not 'saga.Context' in self._engine._adaptor_registry :
            self._logger.warn ("no context adaptors found")
            return

        ctx_adaptors = list()
        for schema in   self._engine._adaptor_registry['saga.Context'] :
            for info in self._engine._adaptor_registry['saga.Context'][schema]:
                ctx_adaptors.append(info['adaptor_name'])
                self._contexts += info['adaptor_instance']._get_default_contexts()

        self._logger.debug ("Adding defaults for context adaptors: %s " \
                       % ctx_adaptors)


class Session (saga.base.SimpleBase) :
    """A SAGA Session object as defined in GFD.90.

    A Bliss session has the purpose of scoping the use of security credentials
    for remote operations.  In other words, a session instance acts as
    a container for security L{Context} instances -- Bliss objects (such as
    L{job.Service} or L{filesystem.File}) created in that session will then use
    exactly the security contexts from that session (and no others).

    That way, the session serves two purposes:  (1) it helps Bliss to decide
    which security mechanism should be used for what interaction, and (2) it
    helps Bliss to find security credentials which would be difficult to pick up
    automatically.
    
    The use of a session is as follows:


    Example::


        # define an ssh context
        c = saga.Context()
        c.context_type = 'ssh'
        c.user_cert = '$HOME/.ssh/special_id_rsa'
        c.user_key = '$HOME/.ssh/special_id_rsa.pub'

        # add it to a session
        s = saga.Session
        s.add_context(c)

        # create a job service in this session -- that job service can now
        # *only* use that ssh context. 
        j = saga.job.Service('ssh://remote.host.net/', s)


    The session argument to the L{job.Service} constructor is fully optional --
    if left out, Bliss will use default session, which picks up some default
    contexts as described above -- that will suffice for the majority of use
    cases.

    A session instance exposes a `context` property, which is a list of
    authentication contexts managed by this session.  As the contexts and the
    session are stateless, it is safe to modify this list as needed.  
    """

    # FIXME: session deep copy not implemented

    def __init__ (self, default=True) :
        """
        default: bool
        ret:     None
        """

        saga.base.SimpleBase.__init__ (self)

        # if the default session is expected, we point our context list to the
        # shared list of the default session singleton.  Otherwise, we create
        # a private list which is not populated.
        if default :
            default_session  = _DefaultSession ()
            self.contexts    = default_session._contexts 
        else :
            self.contexts    = []


    # ----------------------------------------------------------------
    #
    def __str__(self):
        """String represenation."""

        return "Registered contexts: %s" % (str(self.contexts))


    # ----------------------------------------------------------------
    #
    def add_context (self, ctx) :
        """
        ctx:     saga.Context
        ret:     None

        Add a security L{Context} to the session.
        It is encouraged to use the L{contexts} property instead. 
        """

        if ctx not in self.contexts :

            # try to initialize that context, i.e. evaluate its attributes and
            # infer additional runtime information as needed
            try :
                ctx._initialize (self)
            except saga.exceptions.SagaException as e :
                msg = "Cannot add context, initialization failed (%s)"  %  str(e)
                raise saga.exceptions.BadParameter (msg)

            # context initialized ok, add it to the session
            self.contexts.append (ctx)


    # ----------------------------------------------------------------
    #
    def remove_context (self, ctx) :
        """
        ctx:     saga.Context
        ret:     None

        Remove a security L{Context} from the session.
        It is encouraged to use the L{contexts} property instead.
        """

        if ctx in self.contexts :
            self.contexts.remove (ctx)


    # ----------------------------------------------------------------
    #
    def list_contexts  (self) :
        """
        ret:     list[saga.Context]
        
        Retrieve all L{Context} objects attached to the session.
        It is encouraged to use the L{contexts} property instead.
        """

        return self.contexts



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

