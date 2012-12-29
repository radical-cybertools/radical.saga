# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA task interface
"""

import pprint
import time
import traceback
import Queue

import saga.exceptions
import saga.attributes

from   saga.utils.threads import Thread, NEW, RUNNING, DONE, FAILED
from   saga.engine.logger import getLogger


NOTASK   =  None    # makes some implementation internal method invocations more readable
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

        # cache for created container instances
        self._containers = {}

        self._logger = getLogger ('saga.task.Container')


    def add (self, t) :

        if not t in self.tasks :
            self.tasks.append (t)



    def remove (self, t) :

        if t in self.tasks :
            self.tasks.delete (t)



    def run (self) :

        if not len (self.tasks) :
            # nothing to do
            return None


        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}


        for container in buckets['containers'] :

            tasks  = buckets['containers'][container]
            threads.append (Thread (container.container_run, tasks))


        for task in buckets['tasks'] :

            threads.append (Thread (task.run))
            

        # wait for all threads to finish
        for thread in threads :
            thread.join ()

            if thread.get_state () == FAILED :
                raise saga.NoSuccess ("thread exception: %s\n%s" \
                        %  (str(thread.get_exception ()),
                            str(thread.get_traceback ())))


    def wait (self, mode=ALL, timeout=-1) :

        if not mode in [ANY, ALL] :
            raise saga.exceptions.BadParameter \
                    ("wait mode must be saga.task.ANY or saga.task.ALL")

        if type (timeout) not in [int, long, float] : 
            raise saga.exceptions.BadParameter \
                    ("wait timeout must be a floating point number (or integer)")

        if not len (self.tasks) :
            # nothing to do
            return None

        if mode == ALL :
            return self._wait_all (timeout)
        else : 
            return self._wait_any (timeout)


    def _wait_any (self, timeout) :

        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}

        for container in buckets['containers'] :

            tasks  = buckets['containers'][container]
            threads.append (Thread (container.container_wait, tasks, mode, timeout))

        
        for task in buckets['tasks'] :

            threads.append (Thread (task.wait, timeout))
            

            # mode == ANY: we need to watch our threads, and whenever one
            # returns, and declare success.  Note that we still need to get the
            # finished task from the 'winner'-thread -- we do that via a Queue
            # object.  Note also that looser threads are not canceled, but left
            # running (FIXME: consider sending a signal at least)
            timeout = 0.01 # seconds
            done    = False

            for thread in threads :
                thread.join (timeout)

                if thread.get_state () == FAILED :
                    raise thread.get_exception ()

                if not thread.isAlive :
                    # thread indeed finished -- dig return value from this
                    # threads queue
                    result = thread.get_result ()

                    # ignore other threads, and simply declare success
                    return result



    def _wait_all (self, timeout) :

        buckets = self._get_buckets ()
        ret     = None

        for container in buckets['containers'] :

            tasks  = buckets['containers'][container]
            container.container_wait (tasks, ALL, timeout)
            ret    = tasks[0]
 
        # all done - return random task
        return ret



    def cancel (self) :

        if not len (self.tasks) :
            # nothing to do
            return None


        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}


        for container in buckets['containers'] :

            tasks  = buckets['containers'][container]
            queue  = Queue.Queue ()
            thread = su_threads.wrap (container.container_cancel, (queue, tasks))

            threads.append (thread)
            queues[thread] = queue


        for task in buckets['tasks'] :

            queue  = Queue.Queue ()
            thread = su_threads.wrap (task.cancel, (queue, timeout))

            threads.append (thread)
            queues[thread] = queue
            

        # wait for all threads to finish
        for thread in threads :
            thread.join ()

            if thread.get_state () == FAILED :
                raise saga.NoSuccess ("thread exception: %s\n%s" \
                        %  (str(thread.get_exception ()),
                            str(thread.get_traceback ())))

    



    def get_size (self) :

        return len(self.tasks)


    def get_tasks (self) :

        return self.tasks


    def get_states (self) :


        if not len (self.tasks) :
            # nothing to do
            return None


        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}


        for container in buckets['containers'] :

            tasks  = buckets['containers'][container]
            threads.append (su_threads.Thread (container.container_get_states, tasks))


        for task in buckets['tasks'] :

            threads.append (su_threads.Thread (task.get_states, timeout))
            

        # wait for all threads to finish
        for thread in threads :
            thread.join ()

            if thread.get_state () == FAILED :
                raise saga.NoSuccess ("thread exception: %s\n%s" \
                        %  (str(thread.get_exception ()),
                            str(thread.get_traceback ())))


        states = []
        for queue in queues :
            result = queue.get ()
            
            # FIXME: check if this was an exception
            states.append (result)

        return states



    def _get_buckets (self) :
        # collective container ops: walk through the task list, and sort into
        # buckets of tasks which have (a) the same task._container, or if that
        # is not set, the same class type (for which one container instance is
        # created).  All tasks were neither is available are handled one-by-one

        buckets = {}
        buckets['tasks']      = [] # no container adaptor for these [tasks]
        buckets['containers'] = {} # dict  of container adaptors : [tasks]

        for t in self.tasks :

            if hasattr (t._adaptor, '_container') and t._adaptor._container :
                # the task's adaptor has a associated container class which can
                # handle the container ops - great!
                c = t._adaptor._container
                if not c in buckets['containers'] :
                    buckets['containers'][c] = []
                buckets['containers'][c].append (t)

            else :

                # we do not have a container -- so we use the class information
                # to create a new instance of the task, and assume that this can
                # act as container class.  We cache that instance for future
                # uses.  Note that this will call the adaptor ctor, but will
                # *not* initialize the adaptor -- the bulk ops need to make sure
                # to get the respective state from the task objects on their
                # own...
                c  = None
                cc = t._adaptor.__class__ 

                if cc in self._containers :
                    # we created a container class instance in the past, and can
                    # reuse it.
                    c = self._containers[cc]
                else :
                    try :
                        c = cc ()
                    except Exception as e :
                        self._logger.warn ("Cannot create container class: %s" %  str(e))
                        
                if c :
                    # we got a container, use it just as above
                    if not c in buckets['container_classes'] :
                        buckets['container_classes'][c] = []
                    buckets['container_classes'][c].append (t)

                else :
                    # we ultimately have no container to handle this task -- so
                    # put it into the fallback list
                    buckets['tasks'].append (t)

        # pprint.pprint (buckets)

        return buckets

