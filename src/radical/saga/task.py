
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Task interface
"""

import inspect

import radical.utils.signatures  as rus
import radical.utils             as ru

from  . import base              as sbase
from  . import exceptions        as se
from  . import attributes        as satt

from .  import constants         as c

STATES = [c.UNKNOWN, c.NEW, c.RUNNING, c.DONE, c.FAILED, c.CANCELED]


# ------------------------------------------------------------------------------
#
class Task (sbase.SimpleBase, satt.Attributes) :

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  'CPIBase', 
                  str,
                  dict, 
                  rus.one_of (c.SYNC, c.ASYNC, c.TASK))
    @rus.returns (rus.nothing)
    def __init__ (self, _adaptor, _method_type, _method_context, _ttype) :
        """ 
        This saga.Task constructor is private.

        ``_adaptor`` references the adaptor class instance from which this task
        was created via an asynchronous function.  Note that the API level
        object instance can be inferred via ``_adaptor.get_api ()``.  Further,
        the adaptor will reference an _adaptor._container class, which will be
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

        If the ``_method_context`` has *exactly* three elements, names
        ``_call``, ``args`` and ``kwargs``, then the created task will wrap
        a :class:`ru.Future` with that ``_call (*_args, **kwargs)``.
        """

        self._base = super  (Task, self)
        self._base.__init__ ()

        self._future         = None
        self._ttype          = _ttype
        self._adaptor        = _adaptor
        self._method_type    = _method_type
        self._method_context = _method_context

        # set attribute interface properties
        self._attributes_extensible    (False)
        self._attributes_allow_private (True)
        self._attributes_camelcasing   (True)

        # register properties with the attribute interface
        self._attributes_register  (c.RESULT,    None,    satt.ANY,  satt.SCALAR, satt.READONLY)
        self._attributes_set_getter(c.RESULT,    self.get_result)
        self._attributes_set_setter(c.RESULT,    self._set_result)

        self._attributes_register  (c.EXCEPTION, None,    satt.ANY,  satt.SCALAR, satt.READONLY)
        self._attributes_set_getter(c.EXCEPTION, self.get_exception)
        self._attributes_set_setter(c.EXCEPTION, self._set_exception)

        self._attributes_register  (c.STATE,     c.UNKNOWN, satt.ENUM, satt.SCALAR, satt.READONLY)
        self._attributes_set_enums (c.STATE,     STATES)
        self._attributes_set_getter(c.STATE,     self.get_state)
        self._attributes_set_setter(c.STATE,     self._set_state)

        self._set_state (c.NEW)

        # check if this task is supposed to wrap a callable in a future
        if  '_call'   in self._method_context :

            call   = self._method_context['call']
            args   = self._method_context.get('_args',   list())
            kwargs = self._method_context.get('_kwargs', dict())

            # if the called function expects a task handle, provide it.
            if  '_from_task' in inspect.getargspec (call)[0] and \
                '_from_task' not in kwargs :
                kwargs['_from_task'] = self

            self._future = ru.Future (call=call, args=args, kwargs=kwargs)


        # ensure task goes into the correct state
        if self._ttype == c.SYNC :
            self.run  ()
            self.wait ()
        elif self._ttype == c.ASYNC :
            self.run  ()
        elif self._ttype == c.TASK :
            pass


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task')
    @rus.returns (rus.nothing)
    def run      (self) :

        if  self._future :
            self._future.run ()

        else :
            # FIXME: make sure task_run exists.  Should be part of the CPI!
            self._adaptor.task_run (self)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  rus.optional (float))
    @rus.returns (bool)
    def wait (self, timeout=None) :

        if timeout is None:
            timeout = -1.0  # FIXME

        if self._future :
            self._future.wait (timeout)  # FIXME: timeout?!
            self._set_state   (self._future.state)

        else :
            # FIXME: make sure task_wait exists.  Should be part of the CPI!
            self._adaptor.task_wait (self, timeout)


    # ----------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  float)
    @rus.returns (rus.nothing)
    def cancel (self) :

        if self._future :
            self._future.cancel ()
            self._set_state (c.CANCELED)

        else :
            # FIXME: make sure task_cancel exists.  Should be part of the CPI!
            self._adaptor.task_cancel (self)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  rus.one_of (c.UNKNOWN, c.NEW, c.RUNNING, c.DONE, 
                              c.FAILED,  c.CANCELED))
    @rus.returns (rus.nothing)
    def _set_state (self, state) :

        if state not in STATES:
            raise se.BadParameter ("invalid task state '%s'" % state)

        self._attributes_i_set (self._attributes_t_underscore (c.STATE),
                                                              state, force=True)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task')
    @rus.returns (rus.one_of (STATES))
    def get_state (self) :

        if self._future :
            self._set_state (self._future.state)

        return self.state


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  rus.anything)
    @rus.returns (rus.nothing)
    def _set_result (self, result) :

        self._attributes_i_set(self._attributes_t_underscore (c.RESULT), result, force=True)
        self._attributes_i_set(self._attributes_t_underscore (c.STATE),  c.DONE, force=True)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task')
    @rus.returns (rus.anything)
    def get_result (self) :

        if self.state not in c.FINAL:
            self.wait ()

        assert (self.state in c.FINAL)

        if  self.state == c.FAILED :
            self.re_raise ()
            return

        if self.state == c.CANCELED :
            raise se.IncorrectState ("get_result() invalid on cancelled tasks")

        if  self.state == c.DONE :

            if  self._future :
                self._set_result (self._future.result)

            return self.result


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  str, 
                  rus.anything)
    @rus.returns (rus.nothing)
    def _set_metric (self, metric, value) :

        self._attributes_i_set (self._attributes_t_underscore (metric),
                                value, force=True)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task', 
                  se.SagaException)
    @rus.returns (rus.nothing)
    def _set_exception (self, e) :
        self._attributes_i_set (self._attributes_t_underscore (c.EXCEPTION),
                                e, force=True)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task')
    @rus.returns (se.SagaException)
    def get_exception (self) :

        if  self._future :
            self._set_exception (self._future.exception)

        return self.exception


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Task')
    @rus.returns (rus.nothing)
    def re_raise (self) :

        if self.exception :
            raise self.exception


# ------------------------------------------------------------------------------
#
class Container (sbase.SimpleBase, satt.Attributes) :

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container')
    @rus.returns (rus.nothing)
    def __init__ (self) :


        self._base = super  (Container, self)
        self._base.__init__ ()

        # set attribute interface properties
        self._attributes_allow_private (True)
        self._attributes_extensible    (False)
        self._attributes_camelcasing   (True)

        # register properties with the attribute interface
        self._attributes_register   (c.SIZE,    0,  satt.INT,  satt.SCALAR, satt.READONLY)
        self._attributes_set_getter (c.SIZE,    self.get_size)

        self._attributes_register   (c.TASKS,   [], satt.ANY,  satt.VECTOR, satt.READONLY)
        self._attributes_set_getter (c.TASKS,   self.get_tasks)

        self._attributes_register   (c.STATES,  [], satt.ENUM, satt.VECTOR, satt.READONLY)
        self._attributes_set_getter (c.STATES,  self.get_states)

        self._attributes_set_enums  (c.STATES,  STATES)

        # cache for created container instances
        self._containers = {}


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :

        ret  = "["
        for task in self.tasks :
            ret += "'%s', "  %  str(task)
        ret += "]"

        return ret


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  Task)
    @rus.returns (rus.nothing)
    def add      (self, task) :

        if  not isinstance (task, Task) :

            raise se.BadParameter ("Container handles tasks, not %s"
                                % (type(task)))

        if task not in self.tasks :
            self.tasks.append (task)



    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  Task)
    @rus.returns (rus.nothing)
    def remove   (self, task) :

        if task in self.tasks :
            self.tasks.delete (task)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container')
    @rus.returns (rus.nothing)
    def run      (self) :

        if not len (self.tasks) :
            # nothing to do
            return None

        buckets = self._get_buckets ()
        futures = []  # futures running container ops

        # handle all container
        for b in buckets['bound'] :

            # handle all methods
            for m in buckets['bound'][b] :

                tasks    = buckets['bound'][b][m]
                m_name   = "container_%s" % m
                m_handle = None

                for (name, handle) in inspect.getmembers (b,
                                                    predicate=inspect.ismethod):
                    if name == m_name :
                        m_handle = handle
                        break

                if not handle :
                    # Hmm, the specified container can't handle the call after
                    # all -- fall back to the unbound handling
                    buckets['unbound'] += tasks

                else :
                    # hand off to the container function, in a separate task
                    futures.append (ru.Future.Run (m_handle, tasks))


        # handle tasks not bound to a container
        for task in buckets['unbound'] :

            futures.append (ru.Future.Run (task.run))


        # wait for all futures to finish
        for future in futures :
            if  future.isAlive () :
                future.join ()

            if  future.state == c.FAILED :
              # print '===='
              # print future.exception
              # print future.traceback
              # print '===='
                raise se.NoSuccess ("future exception: %s" % (future.exception))


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  rus.one_of   (c.ANY, c.ALL),
                  rus.optional (float))
    @rus.returns (rus.list_of (Task))
    def wait (self, mode=c.ALL, timeout=None) :

        if timeout is None:
            timeout = -1.0  # FIXME

        if not len (self.tasks) :
            # nothing to do
            return None

        if mode == c.ALL: return self._wait_all (timeout)
        else            : return self._wait_any (timeout)



    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  float)
    @rus.returns (rus.list_of (Task))
    def _wait_any (self, timeout) :

        buckets = self._get_buckets ()
        futures = []  # futures running container ops

        # handle all tasks bound to containers
        for b in buckets['bound'] :

            # handle all methods -- all go to the same 'container_wait' though)
            tasks = []
            for m in buckets['bound'][b] :
                tasks += buckets['bound'][b][m]

            futures.append (ru.Future.Run (b.container_wait, tasks, c.ANY,
                                           timeout))


        # handle all tasks not bound to containers
        for task in buckets['unbound'] :

            futures.append (ru.Future.Run (task.wait, timeout))


        # mode == ANY: we need to watch our futures, and whenever one
        # returns, and declare success.  Note that we still need to get the
        # finished task from the 'winner'-future -- we do that via a Queue
        # object.  Note also that looser futures are not canceled, but left
        # running (FIXME: consider sending a signal at least)

        timeout = 0.01  # seconds, heuristic :-/

        for future in futures :
            future.join (timeout)

            if future.state == c.FAILED :
                raise future.exception

            if not future.isAlive() :
                # future indeed finished -- dig return value from this
                # futures queue
                result = future.result

                # ignore other futures, and simply declare success
                return result



    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  float)
    @rus.returns (rus.list_of (Task))
    def _wait_all (self, timeout) :
        # this method should actually be symmetric to _wait_any, and could
        # almost be mapped to it, but the code below is a kind of optimization
        # (does not need futures, thus simpler code).

        buckets = self._get_buckets ()
        ret     = None

        # handle all tasks bound to containers
        for b in buckets['bound'] :

            # handle all methods -- all go to the same 'container_wait' though)
            tasks = []
            for m in buckets['bound'][b] :
                tasks += buckets['bound'][b][m]

            # TODO: this is semantically not correct: timeout is applied
            #       n times...
            b.container_wait (tasks, c.ALL, timeout)
            ret = tasks[0]

        # handle all tasks not bound to containers
        for task in buckets['unbound'] :
            task.wait ()
            ret = task

        # all done - return random task (first from last container, or last
        # unbound task)
        # FIXME: that task should be removed from the task container
        return ret


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  rus.optional (float))
    @rus.returns (rus.nothing)
    def cancel   (self, timeout=None) :

        if timeout is None:
            timeout = -1.0  # FIXME

        buckets = self._get_buckets ()
        futures = []  # futures running container ops

        # handle all tasks bound to containers
        for b in buckets['bound'] :

            # handle all methods -- all go to same 'container_cancel' though)
            tasks = []
            for m in buckets['bound'][b] :
                tasks += buckets['bound'][b][m]

            futures.append (ru.Future.Run (b.container_cancel, tasks, timeout))


        # handle all tasks not bound to containers
        for task in buckets['unbound'] :

            futures.append (ru.Future.Run (task.cancel, timeout))


        for future in futures :
            future.join ()


    # ----------------------------------------------------------------
    #
    @rus.takes   ('Container')
    @rus.returns (int)
    def get_size (self) :

        return len (self.tasks)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container', 
                  'basestring')
    @rus.returns (Task)
    def get_task (self, id) :

        # FIXME: this should not be a search, but a lookup
        if not id:
            raise se.NoSuccess ("Lookup requires non-empty id (not '%s')" % id)

        for t in self.tasks:
            if t.id == id:
                return t

        raise se.NoSuccess ("task '%s' not found in container" % id)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container')
    @rus.returns (rus.list_of (Task))
    def get_tasks (self) :

        return self.tasks


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Container')
    @rus.returns (rus.list_of (rus.one_of (*STATES)))
    def get_states (self) :

        buckets = self._get_buckets ()
        futures = []  # futures running container ops

        # handle all tasks bound to containers
        for b in buckets['bound'] :

            # handle all methods -- all go to same 'container_get_states' though
            tasks = []
            for m in buckets['bound'][b] :
                tasks += buckets['bound'][b][m]

            futures.append (ru.Future.Run (b.container_get_states, tasks))


        # handle all tasks not bound to containers
        for task in buckets['unbound'] :

            futures.append (ru.Future.Run (task.get_state))


        # We still need to get the states from all futures.
        # FIXME: order
        states = list()

        for future in futures :
            future.join ()

            if future.state == c.FAILED :
                raise future.exception

            # FIXME: what about ordering tasks / states?
            res = future.result

            if res is not None :
                states.append(res)

        return states


    # ----------------------------------------------------------------
    #
    @rus.takes   ('Container')
    @rus.returns (dict)
    def _get_buckets (self) :
        # collective container ops: walk through the task list, and sort into
        # buckets of tasks which have (a) the same task._container, or if that
        # is not set, the same class type (for which one container instance is
        # created).  All tasks were neither is available are handled one-by-one

        buckets = {}
        buckets['unbound'] = list()  # no container adaptor for these [tasks]
        buckets['bound']   = dict()  # dict  of container adaptors [tasks]

        for task in self.tasks :

            if  task._adaptor and task._adaptor._container :

                # the task's adaptor has a valid associated container class 
                # which can handle the container ops - great!
                b = task._adaptor._container
                m = task._method_type

                if b not in buckets['bound'] :
                    buckets['bound'][b] = {}

                if m not in buckets['bound'][b] :
                    buckets['bound'][b][m] = []

                buckets['bound'][b][m].append (task)

            else :

                # we have no container to handle this task -- so
                # put it into the fallback list
                buckets['unbound'].append (task)

        return buckets


     # FIXME: add get_apiobject

# ------------------------------------------------------------------------------

