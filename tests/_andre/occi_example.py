
# TODO ANDRE:
#   - mail fedcloud list
#     - what size of VMs are available?
#     - how is ssh key configuration on VM instances done?
#   - define application payload for demo (Ashley / Melissa)
#
# TODO MATTEO:
#   - inquire about python code (Italians)
#   - map code snippets to SAGA calls below
#

import saga
import pprint

# connect to EGI endpoint, also connects to EGI marketplace.
# need voms-proxy-init - this should pick up the default X509.
rm = saga.resource.Manager ("fedcloud+occi://remote.host.egi/")

# get names of available EGI-images
templates = rm.list_templates ()

for template in templates :
  pprint.pprint (rm.get_template (template))
  # 
  # type     : Compute
  # template : EGI-WeNMR-Demo1-CESGA
  # image    : i23g24h32
  # size     : 4
  # memory   : 2048
  # 
  # type     : Compute
  # template : EGI-WeNMR-Demo2-CESGA
  # image    : i23g24h32
  # 
  # type     : Compute
  # template : EGI-WeNMR-Demo3-CESGA

# type(vm):  saga.resource.Resource 
vm = rm.acquire ({'template' : 'EGI-WeNMR-Demo3-CESGA'})
print vm.id
print vm.state # NEW

vm.wait (ACTIVE)


js_url = "ssh://" + vm.access # protocol chosen by application
js_url =            vm.access # protocol chosen by adaptor (preferred)

js = saga.job.Service (js_url)

job = js.run_job ("/bin/true")
job.wait ()

# all work is done
vm.cancel ()
vm.wait   (FINAL)

