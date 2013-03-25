
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import time
import saga

try :
    c = saga.Context ('UserPass')
    c.user_id   = 'test_user'
    c.user_pass = 'test_pass'

    s = saga.Session ()
  # s.add_context (c)

    js = saga.job.Service ('ssh://localhost/', session=s)
  
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

