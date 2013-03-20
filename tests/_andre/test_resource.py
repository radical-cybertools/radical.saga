
import saga

cd = saga.resource.ComputeDescription ()
rm = saga.resource.Manager ("shell://localhost")
cr = rm.acquire (cd)
js = saga.job.service (cr.access)
jj  = js.run_job ("/bin/date")

jj.wait ()
cr.release ()

