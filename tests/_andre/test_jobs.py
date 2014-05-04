
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import time
import saga

def my_cb (a, b, c) :
    print " ----- callback: [%s, %s, %s]" % (a, b, c)
    return True

try :
    c = saga.Context ('ssh')
    c.user_key  = '/home/merzky/.ssh/id_rsa_test'
    c.user_id   = 'tester'
    c.user_pass = 'testtest'

    s = saga.Session (default=True)
  # s.add_context (c)


  # js = saga.job.Service ('gsissh://gsissh.kraken.nics.xsede.org', session=s)
  # js = saga.job.Service ('ssh://localhost/', session=s)
    js = saga.job.Service ('ssh://india.futuregrid.org/', session=s)
  
    jd = saga.job.Description ()
    jd.executable = '/bin/echo'
    jd.arguments  = ['hello world; date ; sleep 3']
    jd.output     = "/tmp/out"
    jd.error      = "/tmp/err"
  
    j = js.create_job (jd)
 #  j.add_callback ('State', my_cb)
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
    print "Error: %s" % str(e)




