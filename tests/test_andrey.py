
import saga

js = saga.job.Service("slurm+ssh://localhost/")
jd = saga.job.Description()

jd.executable        = '/bin/sleep'
jd.arguments         = ['10']
jd.wall_time_limit   = ['10']
jd.queue             = "development"
jd.project           = "TG-MCB090174"

N    = 100
jobs = list()

for i in range(N):
    j = js.create_job(jd)
    j.run()
    jobs.append(j)
    print i
print

for j in jobs:
    j.wait()
    print " * %s: %s" % (j.id, j.state)

js.close()

