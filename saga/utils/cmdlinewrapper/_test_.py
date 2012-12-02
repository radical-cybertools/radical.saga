from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper.init_as_subprocess_wrapper()
clw.open()
print clw.run_sync('/bin/date', ['-r 2'])
clw.close()


clw = CommandLineWrapper.init_as_ssh_wrapper(host='gw68.quarry.iu.teragrid.org')
clw.open()
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')

clw.close()

clw = CommandLineWrapper.init_as_gsissh_wrapper(host='lonestar.tacc.utexas.edu')
clw.open()
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')

clw.close()