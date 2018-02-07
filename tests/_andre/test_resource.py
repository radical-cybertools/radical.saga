
import radical.saga as saga


rid = None
rid = '[ec2://aws.amaon.com/]-[i-e239458c]'

c = saga.Context ('ec2_keypair')
c.token = 'futuregrid'

s = saga.Session ()
s.contexts.append (c)

rm = saga.resource.Manager ("ec2://aws.amaon.com/", session=s)
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


if cr and cr.state == saga.resource.ACTIVE :

    js = saga.job.Service (cr.access)
    js.run_job ('hostname')


    

# js = saga.job.Service (cr.access)
# jj  = js.run_job ("/bin/date")
# 
# jj.wait ()
# cr.release ()

