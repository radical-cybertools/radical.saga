
from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper.init_as_ssh_wrapper(host='localhost')

clw.open()

print clw.run_sync('/bin/date')
print clw.run_sync('gcc')

clw.close()

