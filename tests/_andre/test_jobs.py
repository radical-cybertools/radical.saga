
import time
import saga

try :
    js = saga.job.Service ('ssh://root@localhost/')
  
    jd = saga.job.Description ()
    jd.executable = '/bin/sleep'
    jd.arguments  = ['1']
  
    j = js.create_job (jd)
    j.run ()
    print "state: %s" % j.state
    time.sleep (1)
    print "state: %s" % j.state

    while not j.wait (1.0) :
        print "..."

    print "state: %s" % j.state

    # print "list : %s" % js.list ()
    # for id in js.list () :
    #     print "--%s--" % id

except saga.SagaException as e :
    print str(e)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

