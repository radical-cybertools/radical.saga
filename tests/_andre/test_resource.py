
import saga

rm = saga.resource.Manager ("ec2://aws.amaon.com/")

cd = saga.resource.ComputeDescription ()
cr = rm.acquire (cd)
# print cr.access
# js = saga.job.Service (cr.access)
# jj  = js.run_job ("/bin/date")
# 
# jj.wait ()
# cr.release ()

