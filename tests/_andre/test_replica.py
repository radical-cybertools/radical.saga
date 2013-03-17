
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga


def test () :
  try :
  
    lf_1 = saga.replica.LogicalFile ('irods://localhost/some/file/',
                                     saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS)
    lf_1.upload ("file://localhost/tmp/test1")
    
  except saga.exceptions.SagaException as e :
    print "Exception: ==========\n%s"  %  e.get_message ()
    print "%s====================="    %  e.get_traceback ()
    

test ()

