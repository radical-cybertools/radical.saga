
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

