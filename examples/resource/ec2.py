
import os
import sys
import saga

from pudb import set_interrupt_handler; set_interrupt_handler()


rid = None
rid = '[ec2://aws.amaon.com/]-[i-7498d41f]'

c1 = saga.Context ('ec2')
c1.user_id  = os.environ['EC2_ID']
c1.user_key = os.environ['EC2_KEY']

c2 = saga.Context ('ec2_keypair')
c2.token    = 'futuregrid'
c2.user_key = '/home/merzky/.ssh/id_rsa_futuregrid.pub'
c2.user_id  = 'root'

# the above ec2_keypair context should spawn an ssh context, alas this is not
# possible with the present libcloud (see comments in adaptor).  We thus add
# that ssh context manually

c3 = saga.Context ('ssh')
c3.user_id   = 'root'
c3.user_cert = '/home/merzky/.ssh/id_rsa_futuregrid'


s = saga.Session ()
s.contexts.append (c1)
s.contexts.append (c2)
s.contexts.append (c3)

sys.exit (0)

rm = saga.resource.Manager ("ec2://aws.amazon.com/", session=s)
cr = None


if  not rid :

    print rm.list_templates ()
    # print rm.list_images ()
    
    cd = saga.resource.ComputeDescription ()
    cd.image    = 'ami-0256b16b'
    cd.template = 'Small Instance' 
    
    cr = rm.acquire (cd)
    print cr.id
    print cr.state
    print cr.state_detail
    print cr.access
    print cr.description

else :

    cr = rm.acquire (rid)

    print cr.id
    print cr.state
    print cr.state_detail
    print cr.access
    print cr.description


print cr.state
if cr and cr.state == saga.resource.ACTIVE :

    js = saga.job.Service (cr.access, session=s)
    j = js.run_job ('sleep 10')
    print j.state 
    j.wait ()
    print j.state 


print cr.state
cr.destroy ()
print cr.state
    

# js = saga.job.Service (cr.access)
# jj  = js.run_job ("/bin/date")
# 
# jj.wait ()
# cr.release ()

