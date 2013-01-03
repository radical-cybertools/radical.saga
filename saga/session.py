
from   saga.utils.singleton import Singleton
from   saga.base            import SimpleBase

class _DefaultSession (object) :

    __metaclass__ = Singleton

    def __init__ (self) :

        # FIXME: the default session should attempt to pick up default
        # contexts, from all context adaptors

        self._contexts = []



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

