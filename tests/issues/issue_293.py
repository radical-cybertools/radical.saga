import saga

d = saga.filesystem.Directory("sftp://stampede.tacc.utexas.edu/etc/")
d.copy('passwd', 'file://localhost/tmp/')

