
from   saga.utils.singleton import Singleton
from   saga.utils.logger    import getLogger
from   saga.engine.engine   import getEngine
from   saga.base            import SimpleBase

class _DefaultSession (object) :

    __metaclass__ = Singleton

    def __init__ (self) :

        # the default session picks up default contexts, from all context
        # adaptors.  To implemented, we have to do some legwork: get the engine,
        # dig through the registered context adaptors, and ask each of them for
        # default contexts.

        self._contexts = []
        self._engine   = getEngine ()
        self._logger   = getLogger ('saga._DefaultSession')

        if not 'saga.Context' in self._engine._adaptor_registry :
            self._logger.warn ("no context adaptors found")
            return

        for schema in   self._engine._adaptor_registry['saga.Context'] :
            for info in self._engine._adaptor_registry['saga.Context'][schema] :
                self._logger.debug ("pulling defaults for context adaptors : %s [%s]"
                               % (info['adaptor_name'], schema))

                self._contexts += info['adaptor_instance']._get_default_contexts ()



class Session (SimpleBase) :

    def __init__ (self, default=True) :
        '''
        default: bool
        ret:     None
        '''

        SimpleBase.__init__ (self)

        # if the default session is expected, we point our context list to the
        # shared list of the default session singleton.  Otherwise, we create
        # a private list which is not populated.
        if default :
            default_session  = _DefaultSession ()
            self.contexts    = default_session._contexts 
        else :
            self.contexts    = []


    def add_context (self, ctx) :
        '''
        ctx:     saga.Context
        ret:     None
        '''

        if ctx not in self.contexts :
            ctx._initialize (self)
            self.contexts.append (ctx)


    def remove_context (self, ctx) :
        '''
        ctx:     saga.Context
        ret:     None
        '''

        if ctx in self.contexts :
            self.contexts.remove (ctx)

    def list_contexts  (self) :
        '''
        ret:     list[saga.Context]
        '''
        return self.contexts



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

