
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
    the GC will call finalize() on the object.  It is the object's responsibility
    to (a) update the timer and (b) check for object state before each activity.
    Also, the object needs to be able to handle a finalize during a single
    operation which exceeds the given timeout.

    Note that the finalize() is called in a separate thread.  The time granularity
    is 10 seconds.
    """

    # TODO:  make time granularity an option
    # TODO:  make default timeout  an option
    # TODO:  allow to lock garbage collection during object operations.
    # FIXME: locks / thread safety: between checking if object is registered and
    #        locking that object's lock...

    __metaclass__ = single.Singleton



    # --------------------------------------------------------------------------
    #
    #
    class _Activity (object) :
        """ This is an activity context manager -- it gets created by
        timeout_gc.active(obj), and on creation locks the object's lock.  The
        __enter__ method thus does not need to lock again, but the release
        method must do so.  

        This makes it imperative that the ``active()`` method is ALWAYS called in
        a ``with`` statement (or, more exactly, that the ``_Activity`` instances
        returned by ``active()`` are always used in a ``with`` statement --
        otherwise the lock gets never unlocked.
        """

        def __init__ (self, obj) :

            self.obj  = obj
            self.gc   = TimeoutGC ()

            if not obj in self.gc.objects :
                raise saga.BadParameter ("object unknown to GC (%s)" % obj)

            self.lock = self.gc.objects[obj]['lock']
            self.lock.acquire ()

        def __enter__ (self) :
            pass

        def __exit__ (self, type, value, traceback) :
            self.lock.release ()

    # --------------------------------------------------------------------------
    #
    #
    def __init__ (self) :

        self.objects   = {}
        self.run       = True
        self.thread    = sthread.Thread.Run (self._gc)

        # print "gc init"


    # --------------------------------------------------------------------------
    #
    #
    def _gc (self) :

        while self.run :

            now = time.time ()

            for obj in self.objects :

                # lock the object's enty -- otherwise main might remove entries
                # we operate on...

                with self.objects[obj]['lock'] :

                    try :
                        pdict = self.objects[obj]
                        # print "gc collect ? %s [%s]" % (obj, now - pdict['timestamp'])

                        # don't beat a dead horse...
                        if pdict['alive'] :

                            if (now - pdict['timestamp']) > pdict['timeout'] :

                                # print "gc collected %s" % obj
                                obj.finalize ()
                                # print "gc collected ! %s" % obj
                                pdict['alive'] = False

                                # the above may have take some time -- refresh timestamp
                                now = time.time ()

                    except Exception as e :
                        # print "gc oops: %s" % e
                        pass

            # check all objects -- time to idle for a bit...
            time.sleep (1)


    # --------------------------------------------------------------------------
    #
    #
    def register (self, obj, obj_initialize, obj_finalize, timeout=3) :
        """ We do *not* call initialize() on unregistering objects """

        # print "gc register %s" % obj

        obj_lock = threading.RLock ()

        with obj_lock :

            self.objects[obj] = {}
            self.objects[obj]['lock']       = obj_lock
            self.objects[obj]['alive']      = True
            self.objects[obj]['initialize'] = obj_initialize
            self.objects[obj]['finalize']   = obj_finalize
            self.objects[obj]['timeout']    = timeout
            self.objects[obj]['timestamp']  = time.time ()
            self.objects[obj]['reviving']   = False


    # --------------------------------------------------------------------------
    #
    #
    def unregister (self, obj) :
        """ We do *not* call finalize() on unregistering objects """

        # print "gc unregister %s" % obj

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        # remove all traces of the object.  Once released, the lock will have no
        # reference anymore, and will be python-gc'ed, too.
        with self.objects[obj]['lock'] :
            del (self.objects[obj])


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

        # print "gc active %s" % obj

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)


        # reference anymore, and will be python-gc'ed, too.
        with self.objects[obj]['lock'] :

            # make sure the object is alive
            self.revive (obj)

            # print "gc active 1 %s" % obj

            # we have the rlock, so object cannot die.  Create an activity
            # context, which will keep the lock up, and return it.  The lock
            # will get release when the Activitie's __exit__() method is called.
            return self._Activity (obj)


    # --------------------------------------------------------------------------
    #
    #
    def revive (self, obj, timeout=None) :
        """ set as alive and refresh timestamp """

        # print "gc revive %s" % obj

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        with self.objects[obj]['lock'] :
        
            # avoid circular initialization on revival
            if self.objects[obj]['reviving'] :
                return

            try :
                self.objects[obj]['reviving'] = True

                # re-initialize the object, if needed
                if not self.objects[obj]['alive'] :
                    # print "gc revive 1 %s" % obj
                    self.objects[obj]['initialize']()
                    self.objects[obj]['alive'] = True
                    
                    # print "gc revived %s" % obj

            finally :
                self.objects[obj]['reviving'] = False

            # refresh state
            return self.refresh (obj, timeout)



    # --------------------------------------------------------------------------
    #
    #
    def refresh (self, obj, timeout=None) :
        """ refresh timestamp, and possibly set new timeout """

        # print "gc refresh %s" % obj

        # make sure we know that obj
        if not obj in self.objects :
            raise saga.BadParameter ("object unknown to GC (%s)" % obj)

        
        with self.objects[obj]['lock'] :

            # print "gc refreshing %s" % obj
            if not self.objects[obj]['alive'] :
                # too late... 
                return False

            if timeout :
                self.objects[obj]['timeout'] = timeout
            self.objects[obj]['timestamp']   = time.time ()

            return True




# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

