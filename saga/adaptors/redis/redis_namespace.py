
""" A collection of utilities which maps a namespace structure to redis nosql
keys.  It mirrors the Python ``os`` API, to some extent.

The redis key layout is like the following::

    # namespace tree
    /node:/                  : { type : DIR,   ... }
    /node:/etc/              : { type : DIR,   ... }
    /node:/etc/passwd        : { type : ENTRY, ... }
    /node:/etc/pass          : { type : LINK,  tgt : /etc/passwd, ... }
    ...

    # attribute storage for ns entries, aka adverts (incl. object storage)
    /data:/etc/passwd        : { _obj : <serial>, key_1 : val_1, key_2 : val_2, ... }
    ...

    # index for ls type ops
    /kids:/                  : [etc, usr, lib, ...]
    /kids:/etc/              : [init.d, rc.0, rc.1, ...
    ...

    # the two below are only enabled on 'attribute_indexing = True
    # index for attribute key lookups
    /keys:/etc/passwd:key_1  : [/vals/etc/passwd, ...]
    /keys:/etc/passwd:key_2  : [/vals/etc/passwd, ...]

    # index for attribute value lookups
    /vals:/etc/passwd:val_1  : [/keys/etc/passwd, ...]
    /vals:/etc/passwd:val_2  : [/keys/etc/passwd, ...]

all wildcard lookup versions will be slow -- only solution (iiuc) would be to
blow the indexes to cover for wirldcard expansion - which is always incomplete
anyway...

All parts of the above structure are set/get in a single pipelined transactional
'MULTI' block.  One could consider to move the ops into a lua script, if
performance is insufficient.  

TODO:
    - use locks to make thread safe

"""

import re
import os
import time
import string
import redis
import threading

import redis_cache

from   saga.exceptions       import *
from   saga.advert.constants import *
from   saga.engine.logger    import getLogger


TYPE   = 'type'
OBJECT = 'object'

DIR    = 'dir'
ENTRY  = 'entry'
LINK   = 'link'

NODE   = 'node'
DATA   = 'data'
KIDS   = 'kids'
KEYS   = 'keys'
VALS   = 'vals'

MON    = 'saga-advert-events'

# --------------------------------------------------------------------
#
class redis_ns_monitor (threading.Thread) :

    def __init__ (self, r, pub) :

        self.r      = r
        self.pub    = pub
        self.logger = r.logger

        self.pat = {}
        self.pat['ATTRIBUTE'] = re.compile ('\s*\[(?P<key>[^=]+)=(?P<val>.+)]\s*')

        threading.Thread.__init__ (self)
        self.setDaemon (True)


    def run (self) :
        
        callbacks = self.r.callbacks
        sub       = self.pub.listen ()

        while sub :
            
            info = sub.next ()
            data = info['data']

            if not type (data) == type ("") :
                self.logger.debug ("ignoring event : %s"  %  data)
                continue

            # FIXME: need proper regex parsing
            elems = data.split ()
            if not len (elems) == 3 :
                self.logger.debug ("ignoring event args : %s"  %  data)
                continue


            event = elems[0]
            path  = elems[1]
            args  = elems[2:]

            if path in callbacks :
                
                if event == 'ATTRIBUTE' :

                    for arg in args :
                        
                        # args are formatted like '[key=val]'
                        match = self.pat[event].match (string.join (args, ' '))

                        if  not match :
                            self.logger.warn ("event parse error for %s" % args)
                            pp (match)
                            continue

                        # FIXME: error check
                        key     = match.group ('key')
                        val     = match.group ('val')

                        if key in callbacks[path] :
                            for idx in callbacks[path][key] :

                                cb  = callbacks[path][key][idx][0]
                                obj = callbacks[path][key][idx][1]

                                cb (obj, key, val)

                if event == 'ATTRIBUTES' :
                    self.logger.warn ("unknown event type %s" % event)
                    pass


# --------------------------------------------------------------------
#
class redis_ns_server (redis.Redis) :

    def __init__ (self, url) :

        self.url        = url
        self.host       = 'localhost'
        self.port       = 6379
        self.db         = 0
        self.password   = None
        self.errors     = 'strict'

        if url.host     : self.host     = url.host
        if url.port     : self.port     = url.port
        if url.username : self.username = url.username
        if url.password : self.password = url.password

        # create redis client 
        t1 = time.time ()
        redis.Redis.__init__   (self, 
                                host      = self.host,
                                port      = self.port,
                                db        = self.db,
                                password  = self.password,
                                 errors    = self.errors)
        t2 = time.time ()

        # add a logger 
        self.logger = getLogger ("redis-%s"  % self.host)

        # create a cache dict and attach to redis client instance.  Cache
        # lifetime is set to 10 times the redis-connect latency.
        self.cache = redis_cache.Cache (logger=self.logger, ttl=((t2-t1)*10))

        # create a second client to manage the (blocking) 
        # pubsub communication for event notifications
        self.r2 = redis.Redis  (host      = self.host,
                                port      = self.port,
                                db        = self.db,
                                password  = self.password,
                                errors    = self.errors)

        # set up pubsub endpoint, and start a thread to monitor channels
        self.callbacks = {}
        self.pub = self.r2.pubsub ()
        self.pub.subscribe (MON)

        self.monitor = redis_ns_monitor (self, self.pub)
        self.monitor.start ()



    def __del__ (self) :

        if self.pub :
            self.pub.unsubscribe (MON)



# --------------------------------------------------------------------
#
class redis_ns_entry :

    def __init__ (self, path=None) :

        self.path = path
        self.node = {TYPE : None}
        self.data = {}
        self.kids = []

    def __str__ (self) :

        return "[%-4s] %-25s %s %s : %s" \
             % (self.node[TYPE], self.path, self.node, self.kids, self.data) 


# --------------------------------------------------------------------
#
def redis_ns_init (url) :

    if url.scheme != 'redis' :
        raise BadParameter ("scheme in url is not supported (%s != redis://...)" %  url)

    return redis_ns_server (url)



# --------------------------------------------------------------------
#
def redis_ns_parent (r, path) :

    # for some reason, posix allows two leading slashes...
    if path == '/' or path == '//' :
        return '/'

    (dirname, entryname) = os.path.split (path)

    return dirname


# --------------------------------------------------------------------
#
def redis_ns_name (r, path) :

    # for some reason, posix allows two leading slashes...
    if path == '/' or path == '//' :
        return '/'

    (dirname, entryname) = os.path.split (path)

    return entryname


# --------------------------------------------------------------------
#
def redis_ns_data_get (r, path) :

    r.logger.debug ("redis_ns_data_get %s" % path)

    ret = r.cache.get (DATA+':'+path, r.hgetall, DATA+':'+path)

    return ret


# --------------------------------------------------------------------
#
def redis_ns_data_set (r, path, data) :

    r.logger.debug ("redis_ns_data_set %s: %s" % (path, data))

    old_data = redis_ns_data_get (r, path)
    now      = time.time()

    p = r.pipeline ()
    p.hmset  (NODE+':'+path, {'mtime': now})
    p.delete (DATA+':'+path)          # delete old data hash
    p.hmset  (DATA+':'+path, data)    # replace with new one

    # delete old invalid index entries
    # NOTE: one could optimize the delete by checking 'data'...
    for key in old_data :
        val = old_data[key]
        p.srem (KEYS+':'+str(key), path)
        p.srem (VALS+':'+str(val), path)

    # add new index entries
    for key in data :
        val = data[key]
        p.sadd (KEYS+':'+str(key), path)
        p.sadd (VALS+':'+str(val), path)

    # FIXME: eval vals
    p.execute ()

    # issue notification about attribute changes
    args=[]
    for key in data :
        arg.append (key+'='+data[key])

    r.publish (MON, "ATTRIBUTES %s [%s]"  % (path, string.join (args, '][')))

    # refresh cache
    r.cache.set (DATA+':'+path, data)


# --------------------------------------------------------------------
#
def redis_ns_datakey_set (r, path, key, val) :

    r.logger.debug ("redis_ns_datakey_set %s: %s" % (path, key))

    old_data = redis_ns_data_get (r, path)
    now      = time.time()

    p = r.pipeline ()
    p.hmset  (NODE+':'+path, {'mtime': now})
    p.hmset  (DATA+':'+path, {key    : val})

    # delete old invalid index entry
    if key in old_data :
        # we keep the key index entry around
        p.srem (VALS+':'+str(old_data[key]), path)
    else :
        # add new key index entry
        p.sadd (KEYS+':'+str(key), path)

    # add new value index entry
    p.sadd (VALS+':'+str(val), path)

    # FIXME: eval vals
    p.execute ()

    # issue notification about entry creation to parent dir
    r.publish (MON, "ATTRIBUTE %s [%s=%s]"  %  (path, key, val))

    old_data[key] = val
    r.cache.set (DATA+':'+path, old_data)


# --------------------------------------------------------------------
#
def redis_ns_get (r, path) :

    r.logger.debug ("redis_ns_get %s" % (path))

    e = redis_ns_entry (path)

    try :
        e.node = r.cache.get (NODE+':'+path)
        e.data = r.cache.get (DATA+':'+path)

        if e.node[TYPE] == DIR :
            e.kids = r.cache.get (KIDS+':'+path)

    except Exception as e :
        # some cache ops failed, so we need to properly fetch data.  We simply
        # fetch all of it, but kids might not exist
        #
        p = r.pipeline ()
        p.hgetall  (NODE+':'+path)
        p.hgetall  (DATA+':'+path)
        p.smembers (KIDS+':'+path)

        vals = p.execute ()
        if len(vals) != 3 :
            return None

        e.node = vals[0]
        e.data = vals[1]
        e.kids = vals[2]

        if not len(e.node) :
            return None

        else :
            r.cache.set (NODE+':'+path, e.node)
            r.cache.set (DATA+':'+path, e.data)
            r.cache.set (KIDS+':'+path, e.kids)

    return e


# --------------------------------------------------------------------
#
def redis_ns_create (r, e, flags=0) :
    """
    This assumes that the target entry does not exist.
    """
    # FIXME: need to ensure this via a WATCH call.

    r.logger.debug ("redis_ns_create %s" % e)

    path   = e.path
    name   = redis_ns_name   (r, path)
    parent = redis_ns_parent (r, path)

    old_data = redis_ns_data_get (r, path)
    now      = time.time()

    e.node['mtime'] = now
    e.node['ctime'] = now

    p = r.pipeline ()
    if len(e.node) : p.hmset  (NODE+':'+path, e.node)
    if len(e.data) : p.hmset  (DATA+':'+path, e.data)
    if len(e.kids) : p.hmset  (KIDS+':'+path, e.kids)

    # add entry as  kid to parent
    if path != '/' :
        p.sadd (KIDS+':'+parent, path)

    # add new index entries
    for key in e.data :
        val = e.data[key]
        p.sadd (KEYS+':'+str(key), path)
        p.sadd (VALS+':'+str(val), path)


    # FIXME: eval vals
    p.execute ()

    # issue notification about entry creation to parent dir

    r.publish (MON, "CREATE %s [%s]"  %  (parent, name))

    if len(e.node) : r.cache.set (NODE+':'+path, e.node)
    if len(e.data) : r.cache.set (DATA+':'+path, e.data)
    if len(e.kids) : r.cache.set (KIDS+':'+path, e.kids)

    return e


# --------------------------------------------------------------------
#
def redis_ns_read_entry (r, path) :

    r.logger.debug ("redis_ns_data_get %s" % path)

    e = redis_ns_entry (path)
    try :
        e.node = r.cache.get (NODE+':'+path)
        e.data = r.cache.get (DATA+':'+path)

        if e.node[TYPE] == DIR :
            e.kids = r.cache.get (KIDS+':'+path)

    except Exception as e :
        # some cache ops failed, so we need to properly fetch data.  We simply
        # fetch all of it, but kids might not exist
        p = r.pipeline ()
        p.hgetall (NODE+':'+path)
        p.hgetall (DATA+':'+path)
        p.hgetall (KIDS+':'+path)
        vals = p.execute

        # FIXME: check vals types

        e.node = vals[0]
        e.data = vals[1]
        e.kids = vals[2]

    return e


# --------------------------------------------------------------------
#
def redis_ns_write_entry (r, e) :

    r.logger.debug ("redis_ns_write_entry %s: %s" % (path, data))

    p = r.pipeline ()
    p.hmset (NODE+':'+e.path, e.node)  # FIXME: for create, only set if not exist
    p.hmset (DATA+':'+e.path, e.data)
    p.hmset (KIDS+':'+e.path, [])      # FIXME: add to parent kids if needed

    for key in e.data :
        val =  e.data[key]
        p.sadd (KEYS+':'+str(key), e.path)
        p.sadd (VALS+':'+str(val), e.path)

    # FIXME: eval retvals
    p.execute ()

    r.cache.set (DATA+':'+path, data)


# --------------------------------------------------------------------
#
def redis_ns_opendir (r, path, flags) :

    r.logger.debug ("redis_ns_opendir %s" % path)

    e = redis_ns_get (r, path)

    if e :

        if not e.node[TYPE] == DIR :
            raise BadParameter ("Cannot open path %s (not a directory)" % path)

    else :

        if  CREATE         & flags or \
            CREATE_PARENTS & flags    :
            e = redis_ns_mkdir (r, path, flags)
        
        else :
            raise BadParameter ("Cannot open path %s (no such directory)" % path)

    return e
        

# --------------------------------------------------------------------
#
def redis_ns_open (r, path, flags) :

    r.logger.debug ("redis_ns_open %s" % path)

    e = redis_ns_get (r, path)

    if e :

        if e.node[TYPE] == DIR :
            raise BadParameter ("Cannot open path %s (is a directory)" % path)

    else :

        if  CREATE         & flags or \
            CREATE_PARENTS & flags    :

            e = redis_ns_entry (path)

            e.node = {}
            e.data = {}
            e.kids = []

            e.node[TYPE] = ENTRY

            e = redis_ns_create (r, e, flags)
        
        else :
            raise BadParameter ("Cannot open path %s (no such entry)" % path)

    return e
        

# --------------------------------------------------------------------
#
def redis_ns_mkdir (r, path, flags) :
    
    r.logger.debug ("redis_ns_mkdir %s" % path)

    e = redis_ns_get (r, path)

    if e :

        if  e.node[TYPE] == DIR :
            if  EXCLUSIVE & flags :
                raise BadParameter ("directory %s exists"  %  path)

            # all is well
            return e

        else :
            raise BadParameter ("mkdir target exists (not a directory): %s" %  path)


    # does not exist, so we in fact should create the dir

    # if / does not exist, we always create it
    if path != '/' :

      # if CREATE_PARENTS is set, we need to check all parents, and need to create
      # them as needed.  We go top down, and terminate once a parent is found
      if CREATE_PARENTS & flags :

          # this will recursively travel down the chimney hole...
          parent = redis_ns_parent (r, path)
          pe     = redis_ns_mkdir  (r, parent, CREATE_PARENTS)


      else :
      # if 'CREATE_PARENTS is not set, parent must exist.
          parent = redis_ns_parent (r, path)
          pe     = redis_ns_get    (r, parent)

          if not pe :
              raise BadParameter ("mkdir fails, parent does not exist: %s" %  path)



    # now we can create the target dir
    e = redis_ns_entry (path)

    e.node = {}
    e.data = {}
    e.kids = []

    e.node[TYPE] = DIR

    e = redis_ns_create (r, e, flags)

    return e


# --------------------------------------------------------------------
#
def redis_ns_callback (r, path, key, id, cb, obj) :
    # FIXME: this needs a shared lock with the monitor thread!

    r.logger.debug ("redis_ns_callback %s : %s" % (path, key))

    if not path in r.callbacks :
        r.callbacks[path] = {}
        if not key in r.callbacks[path] :
            r.callbacks[path][key] = {}

    if id == None :
        del r.callbacks[path][key]


    if cb :
        r.callbacks[path][key][id] = [cb, obj]
    else :
        # cb == None: remove that callback
        del r.callbacks[path][key][id]

  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

