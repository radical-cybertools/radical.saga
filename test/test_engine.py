
import sys
import random
import saga

from   saga.engine.engine import Engine, getEngine, ANY_ADAPTOR


# try :

e = Engine ()
e._dump()

d = saga.filesystem.Directory ('file://localhost/tmp/test1/test1/',
                               saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS)
print d.get_url ()
f = d.open ('passwd')

f._adaptor._dump()

tc = saga.task.Container ()

for i in range (1, 10) :
  t = d.copy ("/etc/passwd", "/tmp/test_a_%04d.bak"  %  i, ttype=saga.task.TASK)
  tc.add (t)


js = saga.job.Service ("fork://localhost")
for i in range (1, 10) :
    jd = saga.job.Description()
    jd.executable  = '/usr/bin/touch'
    jd.arguments   = ["/tmp/test_b_%04d.bak"  %  i]
    tc.add (js.create_job (jd))

tc.run  ()
tc.wait (saga.task.ALL)

for t in tc.tasks :
  # t._attributes_dump ()
  print "%s : %-6s [%s]"  %  (t, t.state, t.exception)

sys.exit (0)

print f.get_size_self ()
t = f.get_size_self (saga.task.ASYNC)
print t.state
print t.result

# f.copy_self ('passwd.bak') 
f.copy_self ('dummy://boskop/tmp/') 


t = saga.filesystem.Directory.create ('file://localhost/tmp/test1/test1/',
                               saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS, saga.task.ASYNC)
print t
print t.state
d2 = t.get_result ()
print d2






# jd     = saga.job.Description ()
# jd.executable = '/bin/date'

# t_1    = saga.job.Service.create ("local://localhost", ttype=saga.task.TASK)
# print str(t_1)

# js_1   = saga.job.Service ("fork://localhost")
# print js_1.get_url ()

# j_1    = js_1.create_job (jd) 
# print str(j_1)
# print j_1.get_id ()

# t_2    = j_1.get_id (ttype=saga.task.TASK)
# print str(t_2)
# print t_2.get_state ()

s = saga.Session ()

c = saga.Context ('MyProxy')
c.user_id   = 'merzky'
c.user_pass = 'I80PudW.'
c.life_time = 1000
c.server    = 'myproxy.teragrid.org:7514'

s.add_context (c)

for ctx in s.contexts :
  print ctx._attributes_dump()




# except saga.exceptions.SagaException as e :
#   print "Exception: ==========\n%s"  %  e.get_message ()
#   print "%s====================="    %  e.get_traceback ()

