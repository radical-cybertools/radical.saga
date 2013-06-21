
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import string
import linecache

_trace_external = False

"""

  idea from http://www.dalkescientific.com/writings/diary/archive/2005/04/20/tracing_python_code.html


  This module will trace all saga level calls, printing each line as it is being
  executed.  It will not print traces for system libraries (i.e. modules which
  are not in the saga namespace), but will indicate when the code descents to
  the system level.

  Python system traces are not following Python's `exec()` call (and
  derivatives), so the resulting trace may be incomplete.  In particular, the
  trace will not walk down onto adaptor level, yet -- later of this utility may
  do that.  Right now it is mostly useful to trace calls *within* adaptors.

  Use like this::

      def my_call (url) :

          import saga.utils.tracer
          saga.utils.tracer.trace ()

          f = saga.filesystem.File (url)
          print f.size()

          saga.utils.tracer.untrace ()

"""


def _tracer (frame, event, arg) :

    global _trace_external

  # if  event == "call" :
    if  event == "line" :

        filename = frame.f_globals["__file__"]
        lineno   = frame.f_lineno
        
        if (filename.endswith (".pyc") or
            filename.endswith (".pyo") ) :
            filename = filename[:-1]

        line = linecache.getline (filename, lineno)
        idx  = string.find       (filename, "/saga/")

        if idx >= 0 :

            name = filename[idx:]
            print "%-60s:%4d: %s" % (name, lineno, line.rstrip ())
            _trace_external = False

        else :

            if not _trace_external :
                print "--> %-56s:%4s: %s" % (filename, lineno, line.rstrip ())
            _trace_external = True


    return _tracer

def trace () :
    sys.settrace (_tracer)

def untrace () :
    sys.settrace (None)

