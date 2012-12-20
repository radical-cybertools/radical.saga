# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA task interface
"""

import time
import traceback

import saga.exceptions
import saga.attributes


SYNC     = 'Sync'
ASYNC    = 'Async'
TASK     = 'Task'

UNKNOWN  = 'Unknown'
NEW      = 'New'
RUNNING  = 'Running'
DONE     = 'Done'
FAILED   = 'Failed'
CANCELED = 'Canceled'

STATE    = 'State'
RESULT   = 'Result'

ALL      = 'All'
ANY      = 'Any'


# --------------------------------------------------------------------
# container attribute names
SIZE   = "Size"
TASKS  = "Tasks"
STATES = "States"

class Async :
    '''
    tagging interface for SAGA classes which implement asynchronous methods.
    '''
    pass


class Task (saga.attributes.Attributes) :


    def __init__ (self) :

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register   (RESULT,  None,    self.ANY, self.SCALAR, self.READONLY)
        self._attributes_set_getter (RESULT,  self.get_result)
        self._attributes_set_setter (RESULT,  self._set_result)

        self._attributes_register   (STATE,   UNKNOWN, self.ENUM, self.SCALAR, self.READONLY)
        self._attributes_set_enums  (STATE,  [UNKNOWN, NEW, RUNNING, DONE, FAILED, CANCELED])
        self._attributes_set_getter (STATE,   self.get_state)
        self._attributes_set_setter (STATE,   self._set_state)
              
        self._set_state (NEW)


    def _set_result (self, result) :
        self._attributes_i_set (self._attributes_t_underscore (RESULT), result, force=True)
        self._attributes_i_set (self._attributes_t_underscore (STATE),  DONE,   force=True)


    def get_result (self) :
        
        if not self.state in [DONE, FAILED, CANCELED] :
            self.wait ()
        
        if self.state == FAILED :
            self.re_raise ()
            return

        if self.state == CANCELED :
            raise saga.exceptions.IncorrectState \
                    ("task.get_result() cannot be called on cancelled tasks")

        if self.state == DONE :
            return self.result


    def _set_state (self, state) :
        if not state in [UNKNOWN, NEW, RUNNING, DONE, FAILED, CANCELED] :
            raise saga.exceptions.BadParameter ("attempt to set invalid task state '%s'" % state)
        self._attributes_i_set (self._attributes_t_underscore (STATE), state, force=True)


    def get_state (self) :
        return self.state


    def wait (self, timeout=-1) :
        # FIXME: implement timeout, more fine grained wait, attribute notification
        while self.state not in [DONE, FAILED, CANCELED] :
            time.sleep (1)


    def run (self) :
        pass


    def cancel (self) :
        self._set_state (CANCELED)


    def _set_exception (self, e) :
        self._attributes_i_set (self._attributes_t_underscore (EXCEPTION), e, force=True)

    def re_raise () :
        if self.exception :
            raise self.exception




# --------------------------------------------------------------------
#
class Container (saga.attributes.Attributes) :


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

