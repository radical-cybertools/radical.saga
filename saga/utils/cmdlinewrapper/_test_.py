from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper (scheme='shell')
clw.open()
print clw.run_sync('/bin/date', ['-r 2'])
clw.close()


clw = CommandLineWrapper (scheme='ssh', host='gw68.quarry.iu.teragrid.org')
clw.open()
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')

clw.close()

clw = CommandLineWrapper (scheme='gsissh', host='lonestar.tacc.utexas.edu')
clw.open()
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')
print clw.run_sync('/bin/date')

clw.close()

