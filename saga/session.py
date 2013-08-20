
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.utils.singleton
import saga.utils.logger
import saga.engine.engine
import saga.context
import saga.base

import saga.utils.signatures as sus


# ------------------------------------------------------------------------------
#
class _ContextList (list) :
    """
    The `saga.Session` class has a 'contexts' member which is a mutable list of
    `saga.Context` instances.  Adding contexts to that list is semantically
    equivalent to calling `saga.Session.add_context (ctx)`, which (a) creates
    a deep copy of the context, and (b) initializes the context (i.e. calls
    `ctx._initialize (self)`, with `self` being the session instance).  We thus
    create our own provate `_ContextList` class which inherits from the native
    Python `list` class, and overload the `append()` call with said semantics.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, *args, **kwargs) :

        base_list = super  (_ContextList, self)
        base_list.__init__ (*args, **kwargs)


    # --------------------------------------------------------------------------
    #
    def append (self, session, ctx) :

        if  not isinstance (ctx, saga.Context) :
            raise TypeError, "appended item is not a saga.Context instance"

        # create a deep copy of the context (this keeps _adaptor etc)
        ctx_clone = saga.Context  (ctx.type)
        ctx._attributes_deep_copy (ctx_clone)

        # from pudb import set_trace; set_trace()

        # try to initialize that context, i.e. evaluate its attributes and
        # infer additional runtime information as needed
        session._logger.debug ("adding  context : %s" \
                            % (ctx_clone))
        try :
            ctx_clone._initialize (session)
        except saga.exceptions.SagaException as e :
            msg = "Cannot add context, initialization failed (%s)"  %  str(e)
            raise saga.exceptions.BadParameter (msg)

        # context initialized ok, add it to the list of known contexts
        super (_ContextList, self).append (ctx_clone)



# ------------------------------------------------------------------------------
#
class _DefaultSession (object) :

    __metaclass__ = saga.utils.singleton.Singleton

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('_DefaultSession')
    @sus.returns (sus.nothing)
    def __init__ (self) :

        # the default session picks up default contexts, from all context
        # adaptors.  To implemented, we have to do some legwork: get the engine,
        # dig through the registered context adaptors, and ask each of them for
        # default contexts.

        self.contexts  = _ContextList ()
        self._logger   = saga.utils.logger.getLogger ('saga.DefaultSession')

        _engine = saga.engine.engine.Engine ()

        if not 'saga.Context' in _engine._adaptor_registry :
            self._logger.warn ("no context adaptors found")
            return

        for schema in   _engine._adaptor_registry['saga.Context'] :
            for info in _engine._adaptor_registry['saga.Context'][schema] :

                default_ctxs = info['adaptor_instance']._get_default_contexts ()

                for default_ctx in default_ctxs :

                    self.contexts.append (session=self, ctx=default_ctx)
                    self._logger.debug   ("default context [%-20s] : %s" \
                                     %   (info['adaptor_name'], default_ctx))


# ------------------------------------------------------------------------------
#
class Session (saga.base.SimpleBase) :
    """A SAGA Session object as defined in GFD.90.

    A SAGA session has the purpose of scoping the use of security credentials
    for remote operations.  In other words, a session instance acts as
    a container for security Context instances -- SAGA objects (such as
    job.Service or filesystem.File) created in that session will then use
    exactly the security contexts from that session (and no others).

    That way, the session serves two purposes:  (1) it helps SAGA to decide
    which security mechanism should be used for what interaction, and (2) it
    helps SAGA to find security credentials which would be difficult to pick up
    automatically.
    
    The use of a session is as follows:


    Example::


        # define an ssh context
        c = saga.Context()
        c.context_type = 'ssh'
        c.user_cert = '$HOME/.ssh/special_id_rsa'
        c.user_key  = '$HOME/.ssh/special_id_rsa.pub'

        # add it to a session
        s = saga.Session
        s.add_context(c)

        # create a job service in this session -- that job service can now
        # *only* use that ssh context. 
        j = saga.job.Service('ssh://remote.host.net/', s)


    The session argument to the L{job.Service} constructor is fully optional --
    if left out, SAGA will use default session, which picks up some default
    contexts as described above -- that will suffice for the majority of use
    cases.

    A session instance exposes a `context` property, which is a list of
    authentication contexts managed by this session.  As the contexts and the
    session are stateless, it is safe to modify this list as needed.  
    """

    # FIXME: session deep copy not implemented

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Session', 
                  sus.optional(bool))
    @sus.returns (sus.nothing)
    def __init__ (self, default=True) :
        """
        default: bool
        ret:     None
        """

        simple_base = super  (Session, self)
        simple_base.__init__ ()

        # if the default session is expected, we point our context list to the
        # shared list of the default session singleton.  Otherwise, we create
        # a private list which is not populated.

        if  default :
            default_session  = _DefaultSession ()
            self.contexts    = default_session.contexts 
        else :
            self.contexts    = _ContextList (session = self)


    # ----------------------------------------------------------------
    #
    @sus.takes   ('Session')
    @sus.returns (basestring)
    def __str__  (self):
        """String represenation."""

        return "Registered contexts: %s" % (str(self.contexts))


    # ----------------------------------------------------------------
    #
    @sus.takes      ('Session', 
                     saga.context.Context)
    @sus.returns    (sus.nothing)
    def add_context (self, ctx) :
        """
        ctx:     saga.Context
        ret:     None

        Add a security L{Context} to the session.
        It is encouraged to use the L{contexts} property instead. 
        """

        return self.contexts.append (session=self, ctx=ctx)


    # ----------------------------------------------------------------
    #
    @sus.takes   ('Session', 
                  saga.context.Context)
    @sus.returns (sus.nothing)
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
    @sus.takes   ('Session')
    @sus.returns (sus.list_of (saga.context.Context))
    def list_contexts  (self) :
        """
        ret:     list[saga.Context]
        
        Retrieve all L{Context} objects attached to the session.
        It is encouraged to use the L{contexts} property instead.
        """

        return self.contexts



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

