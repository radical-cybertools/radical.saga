
import time

from   pprint             import pprint
from   threading          import Lock
from   redis_ordered_dict import OrderedDict

CACHE_DEFAULT_SIZE = 10000
CACHE_DEFAULT_TTL  = 1.0    # 1 second

VAL = 'val'
TTL = 'ttl'

######################################################################
#
class Cache :

    # ----------------------------------------------------------------
    #
    def __init__ (self, logger, size=CACHE_DEFAULT_SIZE, ttl=CACHE_DEFAULT_TTL) :

        if int (size) < 1 :
            raise AttributeError ('size < 1 or not a number')

        self.size   = size
        self.ttl    = ttl
        self.dict   = OrderedDict ()
        self.lock   = Lock ()
        self.logger = logger

        print " ------------------ "
        print self.ttl

        # start a thread which, with low priority, cleans out the dict now and
        # then (pops items until a live one is found

    # ----------------------------------------------------------------
    #
    def get (self, key, func=None, *args, **kwargs) :

        self.logger.info ("redis_cache_get %s", key)

        with self.lock:

            # check if we have a live entry
            if key in self.dict :
                if self.dict[key][TTL] > time.time () :
                    # if yes, cache hit!
                    # return data -- doh!
                    # print "!"
                    return self.dict[key][VAL]


            # no live cache entry, check if we can refresh the cache
            if not func :
                # no means to refresh, raise an exception
                del self.dict[key]
                raise AttributeError ("cache miss for '%s' " % key)
            else :
                # cache miss
                # refresh cached value
                # print "?"
                ret = func (*args, **kwargs)

                # set wants lock, so we rather push data ourself here
                self.dict[key]      = {}
                self.dict[key][VAL] = ret
                self.dict[key][TTL] = time.time () + self.ttl

                return ret


    # ----------------------------------------------------------------
    #
    def set (self, key, value) :

        with self.lock :
            while len (self.dict) >= self.size :
                self.dict.popitem (last=False)

            self.dict[key]      = {}
            self.dict[key][VAL] = value
            self.dict[key][TTL] = time.time () + self.ttl


    # ----------------------------------------------------------------
    #
    def delete (self, key) :

        with self.lock :
            del self.dict[key]




## ######################################################################
## #
## def memoize (key) :
##     def _decorating_wrapper (func) :
##         def _caching_wrapper (*args, **kwargs) :
## 
##             cache_key = normalize_key (key, args, kwargs)
##             now       = time.time ()
## 
##             # if still valid, return it
##             if _times.get (cache_key, now) > now :
##                 return _cache[cache_key]
## 
##             # otherwise store the retval of the decorated function
##             ret = func (*args, **kwargs)
##             _cache[cache_key] = ret
##             _times[cache_key] = now + TTL
##             return ret
##         return _caching_wrapper
##     return _decorating_wrapper



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

