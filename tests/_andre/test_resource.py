
import saga

rid = None
rid = '[ec2://aws.amaon.com/]-[i-4643f22a]'

rm = saga.resource.Manager ("ec2://aws.amaon.com/")

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



    

# js = saga.job.Service (cr.access)
# jj  = js.run_job ("/bin/date")
# 
# jj.wait ()
# cr.release ()

