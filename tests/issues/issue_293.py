from __future__ import absolute_import
import saga

d = saga.filesystem.Directory("sftp://stampede.tacc.utexas.edu/etc/")
d.copy('passwd', 'file://localhost/tmp/')

