
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA task interface """

import saga.exceptions
import saga.attributes
import saga.task

# --------------------------------------------------------------------
# attribute names
SIZE   = "Size"
TASKS  = "Tasks"
STATES = "States"


class TaskContainer (saga.attributes.Attributes) :

    def __init__ (self) :

        # set attribute interface properties
        self._attributes_allow_private (True)
        self._attributes_extensible    (False)
        self._attributes_camelcasing   (True)

        # register properties with the attribute interface
        self._attributes_register   (SIZE,    0,    self.INT, self.SCALAR, self.READONLY)
        self._attributes_set_getter (SIZE,    self.get_size)

        self._attributes_register   (TASKS,   [],    self.ANY, self.VECTOR, self.READONLY)
        self._attributes_set_getter (TASKS,   self.get_tasks)

        self._attributes_register   (STATES,  [],    self.ENUM, self.VECTOR, self.READONLY)
        self._attributes_set_getter (STATES,  self.get_states)

        self._attributes_set_enums  (STATES,  [UNKNOWN, NEW, RUNNING, DONE, FAILED, CANCELED])


    def add (self, t) :

        if not t in self.tasks :
            self.tasks.append (t)


    def remove (self, t) :

        if t in self.tasks :
            self.tasks.delete (t)


    def run (self) :

        # this needs to do bulk ops!
        for t in self.tasks :
            t.run ()

    
    def wait (self, mode) :

        # this needs to do bulk ops!
        # FIXME: resepct mode = ALL, ANY
        for t in self.tasks :
            t.wait ()


    def size (self) :

        return len(self.tasks)


    def get_tasks (self) :

        return self.tasks


    def get_states (self) :

        # this needs to do bulk ops!

        states = []

        for t in self.tasks :
            states.append (t.state)

        return states

