
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

"""

import os
import time
import redis
import redis_cache

from saga.exceptions       import *
from saga.advert.constants import *
from saga.engine.logger    import getLogger


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

    host             = 'localhost'
    port             = 6379
    db               = 0
    password         = None
    socket_timeout   = None
    connection_pool  = None
    charset          = 'utf-8'
    errors           = 'strict'
    decode_responses = False
    unix_socket_path = None

    if url.host     : host     = url.host
    if url.port     : port     = url.port
    if url.username : username = url.username
    if url.password : password = url.password

    t1 = time.time ()
    r  = redis.Redis (host=host,
                      port             = port,
                      db               = db,
                      password         = password,
                      socket_timeout   = socket_timeout,
                      connection_pool  = connection_pool,
                      charset          = charset,
                      errors           = errors,
                      decode_responses = decode_responses,
                      unix_socket_path = unix_socket_path)
    t2 = time.time ()

    # also add a logger to the redis client
    r._logger = getLogger ("redis-%s"  % host)
    r._logger.info ("redis handle initialized")

    # create a cache dict and attach to redis client instance
    r._cache = redis_cache.Cache (logger=r._logger, ttl=((t2-t1)/2))

    # FIXME: make sure the root entries exist

    return r



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

    r._logger.info ("redis_ns_data_get %s" % path)

    ret = r._cache.get (DATA+':'+path, r.hgetall, path)

    return ret


# --------------------------------------------------------------------
#
def redis_ns_data_set (r, path, data) :

    r._logger.info ("redis_ns_data_set %s: %s" % (path, data))

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

    r._cache.set (DATA+':'+path, data)


# --------------------------------------------------------------------
#
def redis_ns_datakey_set (r, path, key, val) :

    r._logger.info ("redis_ns_datakey_set %s: %s" % (path, key))

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

    old_data[key] = val
    r._cache.set (DATA+':'+path, old_data)


# --------------------------------------------------------------------
#
def redis_ns_get (r, path) :

    r._logger.info ("redis_ns_get %s" % (path))

    p = r.pipeline ()
    p.hgetall  (NODE+':'+path)
    p.hgetall  (DATA+':'+path)
    p.smembers (KIDS+':'+path)

    vals = p.execute ()
    if len(vals) != 3 :
        print "Ooops: %s" % str(vals)
        return None

    e = redis_ns_entry (path)
    e.node = vals[0]
    e.data = vals[1]
    e.kids = vals[2]

    if not len(e.node) :
        return None

    else :
        r._cache.set (NODE+':'+path, e.node)
        r._cache.set (DATA+':'+path, e.data)
        r._cache.set (KIDS+':'+path, e.kids)
        return e


# --------------------------------------------------------------------
#
def redis_ns_create (r, e, flags=0) :
    """
    This assumes that the target entry does not exist.
    """
    # FIXME: need to ensure this via a WATCH call.

    r._logger.info ("redis_ns_create %s" % e)

    path   = e.path
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

    if len(e.node) : r._cache.set (NODE+':'+path, e.node)
    if len(e.data) : r._cache.set (DATA+':'+path, e.data)
    if len(e.kids) : r._cache.set (KIDS+':'+path, e.kids)

    return e


# --------------------------------------------------------------------
#
def redis_ns_read_entry (r, path) :

    r._logger.info ("redis_ns_data_get %s" % path)

    e = redis_ns_entry (path)
    try :
        e.node = r._cache.get (NODE+':'+path)
        e.data = r._cache.get (DATA+':'+path)

        if e.node[TYPE] == DIR :
            e.kids = r._cache.get (KIDS+':'+path)

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

    r._logger.info ("redis_ns_data_set %s: %s" % (path, data))

    p = r.pipeline ()
    p.hmset (NODE+':'+e.path, e.node)  # FIXME: for create, only set if not exist
    p.hmset (DATA+':'+e.path, e.data)
    p.hmset (KIDS+':'+e.path, [])      # FIXME: add to parent kids if needed

    for key in e.data :
        val =  e.data[key]
        p.sadd (KEYS+':'+str(key), e.path)
        p.sadd (VALS+':'+str(val), e.path)

    print " --- "
    print p.execute ()
    print " --- "

    r._cache.set (DATA+':'+path, data)


# --------------------------------------------------------------------
#
def redis_ns_opendir (r, path, flags) :

    r._logger.info ("redis_ns_opendir %s" % path)

    e = redis_ns_get (r, path)

    if e :

        if not e.node[TYPE] == DIR :
            raise BadParameter ("Cannot open path %s (not a directory)" % path)

    else :

        if  CREATE         & flags or \
            CREATE_PARENTS & flags    :
            r._logger.info ("redis_ns_opendir : calling mkdir for %s" % path)
            e = redis_ns_mkdir (r, path, flags)
        
        else :
            raise BadParameter ("Cannot open path %s (not such directory)" % path)

    print "opendir: %s" % e

    return e
        

# --------------------------------------------------------------------
#
def redis_ns_mkdir (r, path, flags) :
    
    r._logger.info ("redis_ns_mkdir %s" % path)

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



  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

