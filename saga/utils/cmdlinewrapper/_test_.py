from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper.init_as_subprocess_wrapper(None)
clw.open()
print clw.run_sync('/bin/date', ['-r 2'])
clw.close()


clw = CommandLineWrapper.init_as_ssh_wrapper(None, host='gw68.quarry.iu.tseragrid.org')
clw.open()
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')

clw.close()