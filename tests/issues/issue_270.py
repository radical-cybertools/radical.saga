
import saga

jd             = saga.job.Description()
jd.executable  = '/bin/sleep'
jd.arguments   = ['10']

js = saga.job.Service ('ssh://localhost/')
j  = js.create_job    (jd)
print j.exit_code

