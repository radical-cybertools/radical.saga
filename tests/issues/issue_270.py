
import saga

jd             = saga.job.Description()
jd.executable  = '/bin/sleep'
jd.arguments   = ['1']

js = saga.job.Service ('ssh://localhost/')
j  = js.create_job    (jd)
print j.exit_code
j.run ()
j.wait ()
print j.exit_code

