
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"


#!/usr/bin/python

SYNC  = 'Sync'
ASYNC = 'Async'
TASK  = 'Task'

# ------------------------------------
# exception class
#
class NotImplemented :
  pass

  def __str__ (self) :
    return "NotImplemented"


# ------------------------------------
# decorator, which switches method to 
# _async version if ttype is set and !None
def async (sync_function) :
  def wrap_function (self, *args, **kwargs) :

    if 'ttype' in kwargs :
      if kwargs['ttype'] :
        # TODO: check if ttype is valid enum
        # call async function flavor
        try :
          async_function_name = "%s_async"  %  sync_function.__name__
          async_function      = getattr (self, async_function_name)
        except AttributeError :
          print " %s: async %s() not implemented"  %  (self.__class__.__name__, sync_function.__name__)
          return None
          # raise NotImplemented
        else :
          # 'self' not needed, getattr() returns member function
          return async_function (*args, **kwargs)
    
    # no ttype, or ttype==None: call default sync function
    return sync_function (self, *args, **kwargs)

  return wrap_function


# ------------------------------------
# same decorator, different name
def sync (sync_function) :
  return async (sync_function)


# ------------------------------------
# a cpi class which only has sync methods
class sync_printer (object) :

  @sync
  def print_message (self, msg) :
    print " sync printer: %s"  %   msg


# ------------------------------------
# a cpi class which has async methods
class async_printer (object) :

  @async
  def print_message (self, msg) :
    print "async printer: %s"  %   msg
  
  def print_message_async (self, msg, ttype) :
    print "async printer: %s (%s)"  %   (msg, ttype)


# ------------------------------------
# test the sync class (fails on ttype versions)
sp = sync_printer ()

sp.print_message ('test')
sp.print_message ('test', ttype=SYNC)
sp.print_message ('test', ttype=ASYNC)
sp.print_message ('test', ttype=TASK)


# ------------------------------------
# test the async class
ap = async_printer ()

ap.print_message ('test')
ap.print_message ('test', ttype=SYNC)
ap.print_message ('test', ttype=ASYNC)
ap.print_message ('test', ttype=TASK)



