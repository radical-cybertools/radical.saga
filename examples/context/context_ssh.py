
c = saga.Context ('ssh')
c.user_id   = 'tg12736'
c.user_cert = '/home/user/ssh/id_rsa_xsede' # private key derived from cert

s = saga.Session (default=False)            # create session with no contexts
s.add_context (c)

js = saga.job.Service ('ssh://login1.stampede.tacc.utexas.edu', session=s)
j  = js.run_job ("/bin/true")

