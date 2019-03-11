
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


from   saga.exceptions import *


def test_1 () :
  e = NoSuccess ("main")

  e._add_exception (DoesNotExist     ("no such thing"))
  e._add_exception (PermissionDenied ("go away"))
  e._add_exception (BadParameter     ("wrong param a"))
  e._add_exception (BadParameter     ("wrong param b"))

  E = e._get_exception_stack ()
  
  raise E


def test_2 () :
  test_1 ()
  
def test_3 () :
  test_2 ()
  
def test_4 () :
  test_3 ()
  
def test_5 () :
  test_4 ()
  

try :
  test_5 ()

except SagaException as E :

  for e in E.exceptions :
    print str(e)
    print str(e.traceback)
    print " ================================== "

  print str(E)
  print str(E.traceback)
  print " ---------------------------------- "
  



