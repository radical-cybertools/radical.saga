
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import threading

def wrap (call, args) :
    t = threading.Thread (target=call, args=args)
    t.start ()
    return t

