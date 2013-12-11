
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import time
import saga.utils.timeout_gc as togc

class WatchedClass () :

  def __init__ (self) :

    print "ctor"
    self.initialize ()

    self.gc = togc.TimeoutGC ()
    self.gc.register (self, self.initialize, self.finalize, timeout=5)

  def __del__ (self) :
    print "dtor"
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
    with self.gc.active (self) :
      print "action"
      self.f.write ("action\n")

# main
wc = WatchedClass ()
wc.action ()
time.sleep (20)
wc.action ()




