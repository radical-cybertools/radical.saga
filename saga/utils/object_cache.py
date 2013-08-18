
import saga.utils.threads    as sut
import saga.utils.singleton  as sus
import saga.utils.logger     as slog

# ------------------------------------------------------------------------------
#
class ObjectCache (object) :

    """ This is a singleton object caching class -- it maintains a reference
    counted registry of existing objects."""

    __metaclass__ = sus.Singleton
    _lock         = sut.RLock ('ObjectCache')

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :
        """
        Make sure the object cache dict is initialized, exactly once.
        """

        with self._lock :
            self._cache  = {}
            self._logger = slog.getLogger ('saga.utils.object_cache')



    # --------------------------------------------------------------------------
    #
    def get_obj (self, oid, creator) :
        """
        For a given object id, attempt to retrieve an existing object.  If that
        object exists, increase the reference counter, as there is now one more
        user for that object.  
        
        If that object does not exist, call the given creator, then register and
        return the object thusly created.
        """

        with self._lock :

            oid = str(oid)

            if  not oid in self._cache :

                obj = creator ()

                self._cache [oid]        = {}
                self._cache [oid]['cnt'] = 0
                self._cache [oid]['obj'] = obj

            self._cache [oid]['cnt'] += 1

            self._logger.debug("get %s [%s] [%s]" % (oid, self._cache [oid]['cnt'], self._cache [oid]['obj']))
            return self._cache [oid]['obj']


    # --------------------------------------------------------------------------
    #
    def rem_obj (self, obj) :
        """
        For a given objects instance, decrease the refcounter as the caller
        stops using that object.  Once the ref counter is '0', remove all traces
        of the object -- this should make that object eligable for Python's
        garbage collection.  Returns 'True' if the given object was indeed
        registered, 'False' otherwise.
        """

        with self._lock :

            self._logger.debug("rem %s" % str(obj))

            for oid in self._cache.keys () :

                if  obj == self._cache [oid]['obj'] :

                    self._cache [oid]['cnt'] -= 1

                    self._logger.debug("rem %s [%s' [%s]" % (oid, self._cache [oid]['cnt'], self._cache [oid]['obj']))

                    if  self._cache [oid]['cnt'] == 0 :
                        self._logger.debug("del %s [%s' [%s]" % (oid, self._cache [oid]['cnt'], self._cache [oid]['obj']))
                        self._cache [oid]['obj'] = None  # free the obj reference
                        self._cache.pop (oid, None)      # remove the cache entry

                    return True # obj found

            return False  # obj not found


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

