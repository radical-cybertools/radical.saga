
import saga

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine, ANY_ADAPTOR


class Session (object) :

    def __init__ (self, default=True) :
        '''
        default: bool
        ret:     None
        '''
        self._logger = getLogger ('saga.Session')
        self._logger.debug ("saga.Session.__init__(%s)"  %  default)

        self.contexts = []


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

