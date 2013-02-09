
import time
import threading

import saga.utils.threads   as sthread
import saga.utils.singleton as single


# ------------------------------------------------------------------------------
#
#
class TimeoutGC (object) :
    """
    This class supports a timout driven garbage collection.  A monitored object
    instance can register itself with this garbage collector (a singleton), and
    update its timer whenever it is active.  When being inactive for a while,
    the GC will call finalize() on the object.  The object can notify the gc of
    ongoing activities -- this will (a) reset the timer, and (b) the gc will
    ensure that the object is in a viable state for the activity -- i.e. it will
    call the object's ``initialize()`` method if it timed out before.

    Note that the finalize() is called in a separate thread.  The time granularity
    is 10 seconds -- i.e. the thread checks for idle objects every 10 seconds.

    Usage::

      import time
      import saga.utils.timeout_gc as togc
      
      class WatchedClass () :
      
        def __init__ (self) :
      
          self.initialize ()
      
          self.gc = togc.TimeoutGC ()
          self.gc.register (self, self.initialize, self.finalize)
      
        def __del__ (self) :
          self.gc.unregister (self)
          self.finalize ()
          pass
      
        def initialize (self) :
          print "init"
          self.f = open('/tmp/watched.txt', 'a+')
          self.f.write ("init\n")
      
        def finalize (self) :
          print "fini"
          self.f.write ("fini\n")
          self.f.close ()
      
        def action (self) :
          print "action"
          with self.gc.active (self) :
            self.f.write ("action\n")
      
      # main
      wc = WatchedClass ()
      wc.action ()
      time.sleep (20)
      wc.action ()

    This will print::

      init
      action
      fini
      init
      action
      fini

    The ``sleep(20)`` will cause the ``TimeoutGC``  to call the ``finalize()``
    method of the watched class.  On the next action after the sleep, the ``with
    self.gc.active (self)`` statement will ensure that the object is alive
    again -- the ``TimeoutGC`` will call the object's ``initialize()`` method.


    Known Limitations:
    ^^^^^^^^^^^^^^^^^^

      * Objects which register in the garbage collector are never removed unless
        they unregister.  This can potentially lead to memory starvation, as the
        native Python garbage collector will not be able to reclaim those
        objects.  Thus: unregister on ``__del__``!

      * When an object's ``finalize()`` is called, due to an timeout, and that
        method raises an exception, the ``TimeoutGC`` still assumes that the
        object is dead, and will potentially attempt to revive it.  It is the
        responsibility of the watched object to handle that condition.
    """

    # TODO:  make time granularity a configuration option
    # TODO:  make default timeout  a configuration option


    __metaclass__ = single.Singleton


    # --------------------------------------------------------------------------
    #
    #
    class _Activity (object) :
        """ This is an activity context manager -- it gets created by
        timeout_gc.active(obj), and on creation locks the object's lock.  The
        __enter__ method thus does not need to lock again, but the __exit__
        method must do so.  

        This makes it imperative that the ``active()`` method is ALWAYS called in
        a ``with`` statement (or, more exactly, that the ``_Activity`` instances
        returned by ``active()`` are always used in a ``with`` statement --
        otherwise the lock gets never unlocked (well, ``__del__`` tries to
        mitigate that, but anyway).

        See http://preshing.com/20110920/the-python-with-statement-by-example
        for a good description of context managers and their use in ``with``
        statements.
        """

        # ----------------------------------------------------------------------
        def __init__ (self, obj) :

            self.obj  = obj
            self.gc   = TimeoutGC ()

            if not obj in self.gc.objects :
                raise saga.BadParameter ("object unknown to GC (%s)" % obj)

            self.lock   = self.gc.objects[obj]['lock']
            self.lock.acquire ()
            self.locked = True

        # ----------------------------------------------------------------------
        def __del__ (self) :

            if self.locked :
                # someone abused this class.  Anyway, to avoid even greater
                # problems, we unlock...
                self.lock.release ()

        # ----------------------------------------------------------------------
        def __enter__ (self) :
            # we are locked -- see __init__
            pass

        # ----------------------------------------------------------------------
        def __exit__ (self, type, value, traceback) :
            self.gc.refresh (self.obj)  # work is done, restart idle timer
            self.lock.release ()
            self.locked = False


    # --------------------------------------------------------------------------
    #
    #
    def __init__ (self) :
        """
        Initialize the timeout garbage collector: create the state dict, and
        start the watcher thread.
        """

        self.objects   = {}
        self.run       = True
        self.thread    = sthread.Thread.Run (self._gc)


    # --------------------------------------------------------------------------
    #
    #
    def __init__ (self) :
        """
        On destruction, signal the watcher thread that it can finish.  Well,
        that thread will likely bail out anyway, since the destruction will
        suddenly remove the self members, but hey...  Since the gc is
        a singleton though, at this point everything goes south anyway, so we
        don't care...
        """
        self.run = False

    # --------------------------------------------------------------------------
    #
    #
    def _gc (self) :
        """
        This is the watcher thread: it will run forever, and will regularly
        check idle times for all living registered objects.  Note that it will
        lock the object's lock -- and since this is a separate thread, this will
        actually *lock* and potentially block.  That means that (a) garbage
        collection can be delayed if aquiring that lock is delayed, and (b)
        that the watched object can be blocked if it attempts an activity, but
        this gc thread decided to finalize it.  But hey, that's the idea, right?
        """

        while self.run :

            now = time.time ()

            for obj in self.objects :

                # lock the object's enty -- otherwise the main thread might
                # remove entries we operate on...
                with self.objects[obj]['lock'] :

                    try :
                        pdict = self.objects[obj]

                        # don't beat a dead horse...
                        if pdict['alive'] :

                            if (now - pdict['timestamp']) > pdict['timeout'] :

                                obj.finalize ()
                                pdict['alive'] = False

                                # the above may have taken some time -- refresh timestamp
                                now = time.time ()

                    except Exception as e :
                        # finalize failed -- we assume that the object got the
                        # message anyway, and will continue.
                        pass

            # checked all objects -- time to idle for a bit...
            time.sleep (10)


    # --------------------------------------------------------------------------
    #
    #
    def register (self, obj, obj_initialize, obj_finalize, timeout=30) :
        """ 
        Register an object instance for teimout garbage collection.  The
        ``obj_finalize`` method is called when the object instance seems idle
        for longer than ``timeout`` seconds, the ``obj_initialize`` method is
        called if any new activity is performed after that timeout.

        Calling this method twice on the same object instance will replace the
        previous entry.
        
        We do *not* call ``initialize()`` on registering objects.
        """

        obj_lock = threading.RLock ()

        with obj_lock :

            self.objects[obj] = {}
            self.objects[obj]['lock']       = obj_lock
            self.objects[obj]['initialize'] = obj_initialize
            self.objects[obj]['finalize']   = obj_finalize
            self.objects[obj]['timeout']    = timeout
            self.objects[obj]['timestamp']  = time.time ()
            self.objects[obj]['alive']      = True
            self.objects[obj]['reviving']   = False  # avoid circular revival


    # --------------------------------------------------------------------------
    #
    #
    def unregister (self, obj) :
        """ 
        Finish watching that object instance.
        
        We do *not* call ``finalize()`` on unregistering objects.
        """

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        # remove all traces of the object.  Once released, the lock will have no
        # reference anymore, and will be python-gc'ed, too.
        try :
            with self.objects[obj]['lock'] :
                del (self.objects[obj])
        except :
            # we use try/except to make sure that noone deleted the entry
            # between the above check and the locking...
            pass


    # --------------------------------------------------------------------------
    #
    #
    def active (self, obj) :
        """ whenever a registered object wants to perform an activity which
        requires the object to be initialized, it can do::

          with self.gc.active (self) :
              do_work ()

        active() will ensure that the object is alive, reviving it if
        necessary, and lock it to prevent intermediate closure.
        """

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        with self.objects[obj]['lock'] :

            # make sure the object is alive
            self.revive (obj)

            # we have the rlock, so object cannot be finalized.  Create an
            # activity context, which will keep the lock up, and return it.  The
            # lock will get release when the Activitie's __exit__() method is
            # called.
            return self._Activity (obj)


    # --------------------------------------------------------------------------
    #
    #
    def revive (self, obj) :
        """
        Re-initialize the object if needed, re-set as alive, and refresh
        timestamp. 
        """

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        with self.objects[obj]['lock'] :
        
            # avoid circular initialization on revival
            if self.objects[obj]['reviving'] :
                return

            if not self.objects[obj]['alive'] :
                try :

                    # re-initialize the object, if needed
                    self.objects[obj]['reviving'] = True
                    self.objects[obj]['initialize']()
                    self.objects[obj]['alive']    = True
                except :
                    # revival failed, the object remains dead...
                    # This is bad actually, as the next activity request will
                    # trigger *another* revival attempt -- but we hope that the
                    # activity will, at some point, realize that the object
                    # remained dead -- what was the point of revival otherwise
                    # anyway??
                    pass

                finally :
                    self.objects[obj]['reviving'] = False

            # refresh state (we do that even if the object was not revived)
            return self.refresh (obj)



    # --------------------------------------------------------------------------
    #
    #
    def refresh (self, obj) :
        """ refresh timestamp on live objects """

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        
        with self.objects[obj]['lock'] :

            if self.objects[obj]['alive'] :

                # *uff*, not too late... 
                self.objects[obj]['timestamp'] = time.time ()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

