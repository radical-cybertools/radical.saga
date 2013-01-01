# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA task interface
"""

import pprint
import time
import inspect
import traceback
import Queue

import saga.exceptions
import saga.attributes

import saga.job.job

from   saga.constants     import *
from   saga.utils.threads import Thread, NEW, RUNNING, DONE, FAILED
from   saga.engine.logger import getLogger



class Task (saga.attributes.Attributes) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, _adaptor, _method_type, _method_context, _ttype) :
        """ 
        This saga.Task constructor is private.

        ``_adaptor`` references the adaptor class instance from which this
        task was created via an asynchronous function.  Note that the API level
        object instance can be inferred via ``_adaptor._api``.  Further, the
        adaptor will reference an _adaptor._container class, which will be
        considered the target for bulk operations for this task.

        ``_method_type`` specifies the SAGA API method which task is
        representing.  For example, for the following code::

          d = saga.filesystem.Directory ("file:///")
          t = d.copy ('/etc/passwd', '/tmp/passwd.bak', saga.task.ASYNC)

        The resulting task ``t`` would represent the *'copy'* method.  This is
        required to forward :class:`saga.task.Container` calls to the correct
        bulk method, in this case ``container_copy()``.

        ``_method_context`` describes the context in which the task method is
        running.  It is up to the creator of the task to provide that context --
        in general, it will at least include method parameters.

        ``ttype`` determines in what state the constructor will leave the task:
        ``DONE`` for ``ttype=SYNC``, ``RUNNING`` for ``ttype=ASYNC`` and ``NEW``
        for ``ttype=TASK``.
        """
        
        self._ttype          = _ttype
        self._adaptor        = _adaptor
        self._method_type    = _method_type
        self._method_context = _method_context

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register   (RESULT,    None,    self.ANY, self.SCALAR, self.READONLY)
        self._attributes_set_getter (RESULT,    self.get_result)
        self._attributes_set_setter (RESULT,    self._set_result)

        self._attributes_register   (EXCEPTION, None,    self.ANY, self.SCALAR, self.READONLY)
        self._attributes_set_getter (EXCEPTION, self.get_exception)
        self._attributes_set_setter (EXCEPTION, self._set_exception)

        self._attributes_register   (STATE,     UNKNOWN, self.ENUM, self.SCALAR, self.READONLY)
        self._attributes_set_enums  (STATE,    [UNKNOWN, NEW, RUNNING, DONE, FAILED, CANCELED])
        self._attributes_set_getter (STATE,     self.get_state)
        self._attributes_set_setter (STATE,     self._set_state)
              
        self._set_state (NEW)

        if self._ttype == saga.task.SYNC :
            self.run  ()
            self.wait ()
        elif self._ttype == saga.task.ASYNC :
            self.run  ()
        elif self._ttype == saga.task.TASK :
            pass



    # ----------------------------------------------------------------
    #
    def _set_result (self, result) :
        self._attributes_i_set (self._attributes_t_underscore (RESULT), result, force=True)
        self._attributes_i_set (self._attributes_t_underscore (STATE),  DONE,   force=True)


    # ----------------------------------------------------------------
    #
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


    # ----------------------------------------------------------------
    #
    def _set_state (self, state) :
        if not state in [UNKNOWN, NEW, RUNNING, DONE, FAILED, CANCELED] :
            raise saga.exceptions.BadParameter ("attempt to set invalid task state '%s'" % state)
        self._attributes_i_set (self._attributes_t_underscore (STATE), state, force=True)


    # ----------------------------------------------------------------
    #
    def get_state (self) :
        return self.state


    # ----------------------------------------------------------------
    #
    def wait (self, timeout=-1) :
        # FIXME: make sure task_wait exists.  Should be part of the CPI!
        self._adaptor.task_wait (self, timeout)


    # ----------------------------------------------------------------
    #
    def run (self) :
        # FIXME: make sure task_run exists.  Should be part of the CPI!
        self._adaptor.task_run (self)


    # ----------------------------------------------------------------
    #
    def cancel (self) :
        # FIXME: make sure task_cancel exists.  Should be part of the CPI!
        self._adaptor.task_cancel (self)


    # ----------------------------------------------------------------
    #
    def _set_exception (self, e) :
        self._attributes_i_set (self._attributes_t_underscore (EXCEPTION), e, force=True)

    # ----------------------------------------------------------------
    #
    def get_exception (self) :
        return self.exception

    # ----------------------------------------------------------------
    #
    def re_raise () :
        if self.exception :
            raise self.exception




# --------------------------------------------------------------------
#
class Container (saga.attributes.Attributes) :


    # ----------------------------------------------------------------
    #
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


    # ----------------------------------------------------------------
    #
    def add (self, t) :

        # AM: oh I hate that we don't use proper inheritance...
        if  not isinstance (t, Task) and \
            not isinstance (t, saga.job.job.Job)      :
            
            traceback.print_stack ()
            raise saga.BadParameter ("Container handles jobs or tasks, not %s" \
                                  % (type(t)))

        if not t in self.tasks :
            self.tasks.append (t)



    # ----------------------------------------------------------------
    #
    def remove (self, t) :

        if t in self.tasks :
            self.tasks.delete (t)



    # ----------------------------------------------------------------
    #
    def run (self) :

        if not len (self.tasks) :
            # nothing to do
            return None


        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}


        # handle all container
        for c in buckets['bound'] :
        
            # handle all methods
            for m in buckets['bound'][c] :

                tasks         = buckets['bound'][c][m]
                m_name   = "container_%s" % m
                m_handle = None

                for (name, handle) in inspect.getmembers (c, predicate=inspect.ismethod) :
                    if name == m_name :
                        m_handle = handle
                        break

                if not handle :
                    # Hmm, the specified container can't handle the call after
                    # all -- fall back to the unbound handling
                    buckets['unbound'] += tasks

                else :
                    # hand off to the container function, in a separate task
                    threads.append (Thread (m_handle, tasks))


        # handle tasks not bound to a container
        for task in buckets['unbound'] :

            threads.append (Thread (task.run))
            

        # wait for all threads to finish
        for thread in threads :
            thread.join ()

            if thread.get_state () == FAILED :
                raise saga.NoSuccess ("thread exception: %s\n%s" \
                        %  (str(thread.get_exception ()),
                            str(thread.get_traceback ())))


    # ----------------------------------------------------------------
    #
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



    # ----------------------------------------------------------------
    #
    def _wait_any (self, timeout) :

        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}

        # handle all tasks bound to containers
        for c in buckets['bound'] :

            # handle all methods -- all go to the same 'container_wait' though)
            tasks = []
            for m in buckets['bound'][c] :
                tasks += buckets['bound'][c][m]

            threads.append (Thread (c.container_wait, tasks, mode, timeout))

        
        # handle all tasks not bound to containers
        for task in buckets['unbound'] :

            threads.append (Thread (task.wait, timeout))
            

        # mode == ANY: we need to watch our threads, and whenever one
        # returns, and declare success.  Note that we still need to get the
        # finished task from the 'winner'-thread -- we do that via a Queue
        # object.  Note also that looser threads are not canceled, but left
        # running (FIXME: consider sending a signal at least)

        timeout = 0.01 # seconds, heuristic :-/
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



    # ----------------------------------------------------------------
    #
    def _wait_all (self, timeout) :
        # this method should actually be symmetric to _wait_any, and could
        # almost be mapped to it, but the code below is a kind of optimization
        # (does not need threads, thus simpler code).

        buckets = self._get_buckets ()
        ret     = None

        # handle all tasks bound to containers
        for c in buckets['bound'] :

            # handle all methods -- all go to the same 'container_wait' though)
            tasks = []
            for m in buckets['bound'][c] :
                tasks += buckets['bound'][c][m]

            c.container_wait (tasks, ALL, timeout)
            ret = tasks[0]
 
        # handle all tasks not bound to containers
        for t in buckets['unbound'] :
            t.wait ()
            ret = t

        # all done - return random task (first from last container, or last
        # unbound task)
        return ret



    # ----------------------------------------------------------------
    #
    def cancel (self) :

        if not len (self.tasks) :
            # nothing to do
            return None


        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}


        # handle all tasks bound to containers
        for c in buckets['bound'] :

            # handle all methods -- all go to the same 'container_cancel' though)
            tasks = []
            for m in buckets['bound'][c] :

                tasks += buckets['bound'][c]

            queue  = Queue.Queue ()
            thread = su_threads.wrap (c.container_cancel, (queue, tasks))

            threads.append (thread)
            queues[thread] = queue


        # handle all tasks not bound to containers
        for task in buckets['unbound'] :

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


    # ----------------------------------------------------------------
    #
    def get_size (self) :

        return len (self.tasks)


    # ----------------------------------------------------------------
    #
    def get_tasks (self) :

        return self.tasks


    # ----------------------------------------------------------------
    #
    def get_states (self) :

        if not len (self.tasks) :
            # nothing to do
            return None


        buckets = self._get_buckets ()
        threads = []  # threads running container ops
        queues  = {}


        for container in buckets['bound'] :

            tasks  = buckets['bound'][container]
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



    # ----------------------------------------------------------------
    #
    def _get_buckets (self) :
        # collective container ops: walk through the task list, and sort into
        # buckets of tasks which have (a) the same task._container, or if that
        # is not set, the same class type (for which one container instance is
        # created).  All tasks were neither is available are handled one-by-one

        buckets = {}
        buckets['unbound'] = [] # no container adaptor for these [tasks]
        buckets['bound']   = {} # dict  of container adaptors [tasks]

        for t in self.tasks :

            if  t._adaptor and t._adaptor._container :

                # the task's adaptor has a valid associated container class 
                # which can handle the container ops - great!
                c = t._adaptor._container
                m = t._method_type

                if not c in buckets['bound'] :
                    buckets['bound'][c] = {}

                if not m in buckets['bound'][c] :
                    buckets['bound'][c][m] = []

                buckets['bound'][c][m].append (t)

            else :

                # we have no container to handle this task -- so
                # put it into the fallback list
                buckets['unbound'].append (t)

        return buckets

