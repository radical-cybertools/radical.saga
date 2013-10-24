
import sys
import time
import saga


# ------------------------------------------------------------------------------
#
class my_cb (saga.Callback) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :
        self.t1  = 0
        self.t2  = 0
        self.cnt = 0
    
    # --------------------------------------------------------------------------
    #
    def cb (self, obj, key, val) :
      # print " ----------- callback triggered for %s - %s - %s [%s]" % (obj, key, val, obj.get_attribute (key))
    
        # if the attribute value was set to 'start', we start the timer
        if val == 'start' :
            self.t1 = time.time()
    
        # suprise: if the attribute value was set to 'stop', we stop the timer
        elif val == 'stop' :
            self.t2 = time.time()
        else :
            self.cnt += 1

        # and print the difference when done
        if val == 'stop' :
            diff = self.t2-self.t1
            print '%6d events / %.1f seconds = %.1f events/second' % (self.cnt, diff, self.cnt/diff)
            self.cnt = 0 # reset

        # an callback returning True remains registered for the same event
        return True
    

# ------------------------------------------------------------------------------
#
def test () :

    try :
      
        # Create an advert directory, add an attribute, and listen for events on
        # attribute changes / updates
      # d_1 = saga.advert.Directory ('redis://localhost/tmp/test1/test1/',
        d_1 = saga.advert.Directory ('redis://repex1.tacc.utexas.edu:10001/tmp/test1/test1/',
                                     saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS)

        d_1.set_attribute ('foo', 'bar')
        d_1.add_callback  ('foo', my_cb ())

        while True :
            # tralala
            time.sleep (60)
        

    except saga.exceptions.SagaException as e :
        print "Exception: ==========\n%s"  %  e.get_message ()
        print "%s====================="    %  e.get_traceback ()
    

test ()



