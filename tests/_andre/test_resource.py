
import saga

cd = saga.resource.ComputeDescription ()
rm = saga.resource.Manager ("shell://localhost")
cr = rm.acquire (cd)
print cr.access
js = saga.job.Service (cr.access)
jj  = js.run_job ("/bin/date")

jj.wait ()
cr.release ()

