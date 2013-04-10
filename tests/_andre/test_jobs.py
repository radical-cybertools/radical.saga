
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import time
import saga

try :
    c = saga.Context ('ssh')
    c.user_cert = '/home/merzky/.ssh/id_rsa'
    c.user_key  = '/home/merzky/.ssh/id_rsa.pub'
    c.user_id   = 'tester'
    c.user_pass = 'passpass'

    s = saga.Session (default=False)
    s.add_context (c)

    js = saga.job.Service ('ssh://localhost/bin/sh', session=s)
  
    jd = saga.job.Description ()
    jd.executable = '/bin/echo'
    jd.arguments  = ['hello world; date xxx; sleep 3']
    jd.output     = "/tmp/out"
    jd.error      = "/tmp/err"
  
    j = js.create_job (jd)
    print j.created
    j.run ()
    print j.started
    print "state: %s" % j.state
    time.sleep (1)
    print "state: %s" % j.state

    while not j.wait (1.0) :
        print "..."

    print "state: %s" % j.state
    print j.finished

    # print "list : %s" % js.list ()
    # for id in js.list () :
    #     print "--%s--" % id

except saga.SagaException as e :
    print str(e)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

