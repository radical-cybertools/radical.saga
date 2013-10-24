
import sys
import traceback

# ------------------------------------------------------------------------------
#
class MyEx (Exception) :

    # --------------------------------------------------------------------------
    #
    # the exception constructor always needs a message (no need complaining if
    # there is nothing to complain about), but can also get a parent exception,
    # which usually should indicate the error which triggered *this* exception.
    #
    def __init__ (self, msg, parent=None) :

        ptype = type(parent).__name__   # exception type for parent
        stype = type(self).__name__     # exception type for self, useful for
                                        # inherited exceptions

        # did we get a parent exception?
        if  parent :

            # if so, then this exception is likely created in some 'except'
            # clause, as a reaction on a previously catched exception (the
            # parent).  Thus we append the message of the parent to our own
            # message, but keep the parent's traceback (after all, the original
            # exception location is what we are interested in).
            #
            if  isinstance (parent, MyEx) :
                # that all works nicely when parent is our own exception type...
                self.traceback = parent.traceback

                frame          = traceback.extract_stack ()[-2]
                line           = "%s +%s (%s)  :  %s" % frame 
                self.msg       = "  %-20s: %s (%s)\n%s" % (stype, msg, line, parent.msg)

            else :
                # ... but if parent is a native (or any other) exception type,
                # we don't have a traceback really -- so we dig it out of
                # sys.exc_info.
                trace          = sys.exc_info ()[2]
                stack          = traceback.extract_tb  (trace)
                traceback_list = traceback.format_list (stack)
                self.traceback = "".join (traceback_list)

                # the message composition is very similar -- we just inject the
                # parent exception type inconspicuously somewhere (above that
                # was part of 'parent.msg' already).
                frame          = traceback.extract_stack ()[-2]
                line           = "%s +%s (%s)  :  %s" % frame 
                self.msg       = "  %-20s: %s (%s)\n  %-20s: %s" % (stype, msg, line, ptype, parent)

        else :

            # if we don't have a parent, we are a 1st principle exception,
            # i.e. a reaction to some genuine code error.  Thus we extract the
            # traceback from exactly where we are in the code (the last stack
            # frame will be the call to this exception constructor), and we
            # create the original exception message from 'stype' and 'msg'.
            stack          = traceback.extract_stack ()
            traceback_list = traceback.format_list (stack)
            self.traceback = "".join (traceback_list[:-1])
            self.msg       = "  %-20s: %s" % (stype, msg)


    # convenience method for string conversion -- simply returns message
    def __str__ (self) :

        return self.msg


# ------------------------------------------------------------------------------
#
# inherit a couple of exception types (they'll get the correct 'stype' above)
#
class MyEx_1 (MyEx) : pass
class MyEx_2 (MyEx) : pass
class MyEx_3 (MyEx) : pass


# ------------------------------------------------------------------------------
#
# This is the interesting part -- that call triggers a couple of exceptions.
# Run like this:
#
#   0)  run as is
#   1)  comment out #1 -- run again
#   2)  comment out #2 -- run again
#   3)  comment out #3 -- run again
#
# The four cases above are basically: 
#
#   0)  native exception in code
#   1)  custom exception in code
#   2)  native exception in try
#   3)  custom exception in try
#
# Read on below for more cases though
#
def call_4 () :

    d = int  ('test')     # 1
    e = MyEx_1 ("exception in code_4")
    raise e               # 2
    try :
        d = int  ('test') # 3
        e = MyEx_1 ("exception in try_4")
        raise e
    except Exception as ex :
        e = MyEx_2 ("exception in except_4", ex)
        raise e

# ------------------------------------------------------------------------------
#
# one level up in the call stack, we catch/convert exceptions from call_4
#
def call_3 () :

    # enable this if you want the exceptions to fall through to main.  You can
    # do this for all four cases above.
    # Note that main will only catch 'MyEx' typed exceptions.
    #
    # call_4 ()
    #
    try :
        call_4 ()
    except Exception as ex :
        e = MyEx_3 ("exception in except_3", ex)
        raise e


# ------------------------------------------------------------------------------
# make the call stack a little deeper, for fun
def call_2 () : call_3 () 
def call_1 () : call_2 ()


# ------------------------------------------------------------------------------
#   _________________________________
#  /                                 \
#  |  #    #    ##       #    #    # |
#  |  ##  ##   #  #      #    ##   # |
#  |  # ## #  #    #     #    # #  # |
#  |  #    #  ######     #    #  # # |
#  |  #    #  #    #     #    #   ## |
#  |  #    #  #    #     #    #    # |
#  \                                 /
#   ---------------------------------
#          \   ^__^
#           \  (oo)\_______
#              (__)\       )\/\
#                  ||----w |
#                  ||     ||
#

# enable this if you want to see uncatched exceptions -- you can do this for all
# eight cases above.
#
# call_1 ()   
#
try :
    call_1 ()
except MyEx as e :
    print "=================================="
    print e
    print "=================================="
    print e.traceback
    print "=================================="
    pass



